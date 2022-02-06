open Lwt.Infix
module Header = Cohttp.Header

exception Retry

module Make (IO : S.IO) (Net : S.Net with module IO = IO) (Time : Mirage_time.S) = struct
  module IO = IO
  module Response = Make.Response (IO)
  module Request = Make.Request (IO)

  let src = Logs.Src.create "cohttp.lwt.client" ~doc:"Cohttp Lwt client"

  module Log = (val Logs.src_log src : Logs.LOG)

  type ctx = Net.ctx

  module type ConnectionS = sig
    type t

    val connect :
      ?finalise:(t -> unit Lwt.t) ->
      ?ctx:ctx ->
      Uri.t -> (* XXX this should actually be something like Conduit.endp *)
      t
    val length : t -> int
    val notify : t -> unit Lwt.t
    val close : t -> unit
    val shutdown : t -> unit
    val is_closed : t -> bool
    val request : t -> ?body:Body.t -> Cohttp.Request.t ->
      (Cohttp.Response.t * Body.t) Lwt.t
  end

  module Connection :ConnectionS = struct
    type state =
      | Connecting of (Net.IO.ic * Net.IO.oc) Lwt.t
      | Full of (Net.IO.ic * Net.IO.oc)
      | Closing of (Net.IO.ic * Net.IO.oc)
      | Half of Net.IO.ic
      | Closed
      | Failed of exn
    type req_resr =
      { req :Request.t
      ; body :Body.t
      ; res_r :(Response.t * Body.t) Lwt.u
      ; res :(Response.t * Body.t) Lwt.t }
    type t =
      { mutable state :state
      ; in_flight :req_resr Queue.t
      ; waiting :req_resr Queue.t
      ; condition :unit Lwt_condition.t (* watching queues *)
      ; finalise :(t -> unit Lwt.t)
      ; id :int
      }

    let length connection =
      Queue.length connection.in_flight +
      Queue.length connection.waiting

    let to_string connection =
      Printf.sprintf "Connection %u (%u %s)"
        connection.id
        (length connection)
        (match connection.state with
        |Connecting _ -> "Connecting"
        |Full _ -> "Full"
        |Closing _ -> "Closing"
        |Half _ -> "Half"
        |Closed -> "Closed"
        |Failed _ -> "Failed")

    let log pipeline s = prerr_endline (to_string pipeline ^ ": " ^ s)

    let notify connection =
      Lwt_condition.wait connection.condition

    let queue_fail connection q e =
      prerr_endline "queue_fail";
      Queue.iter (fun {res_r; _} -> Lwt.wakeup_later_exn res_r e) q;
      Queue.clear q;
      Lwt_condition.broadcast_exn connection.condition e

    let close connection =
      log connection "close";
      match connection.state with
      | Connecting channels ->
        connection.state <- Closed;
        Lwt.cancel channels;
        Lwt.on_success channels (fun (ic, oc) -> Net.close ic oc);
        Lwt_condition.broadcast connection.condition ()
      | Closing (ic, oc)
      | Full (ic, oc) ->
        Net.close ic oc;
        connection.state <- Closed;
        Lwt_condition.broadcast connection.condition ()
      | Half ic ->
        Net.close_in ic;
        connection.state <- Closed;
        Lwt_condition.broadcast connection.condition ()
      | Closed | Failed _ -> ()

    let shutdown connection =
      log connection "shutdown";
      match connection.state with
      | Connecting channels ->
        Lwt.async @@ fun () ->
        channels >>= fun channels ->
        (* need to loose possible race with writer *)
        Lwt.pause () >>= fun () ->
        connection.state <- Closing channels;
        Lwt.return_unit
      | Full channels ->
        connection.state <- Closing channels;
      | Closing _ | Half _ | Closed | Failed _ -> ()

    let is_closed connection =
      match connection.state with
      | Full _ -> false
      | Connecting _ -> false
      | Closing _ | Half _ -> true (* really ? *)
      | Closed | Failed _ -> true

    let rec reader connection =
      match connection.state with
      | Connecting _ -> assert false
      | Closed | Failed _ -> assert false
      | Full (ic, _) | Closing (ic, _) | Half ic ->
        log connection "reader waiting";
        Response.read ic >>= fun res ->
        match res with
        | `Ok res ->
          log connection "reader got response";
          (* don't take from queue yet, because body may still be in flight *)
          let {req; res_r; _} = Queue.peek connection.in_flight in

          (* A response header to a HEAD request is indistinguishable from a
           * response header to a GET request. Therefore look at the request. *)
          if
            match Response.has_body res with
            | _ when Request.meth req = `HEAD -> false
            | `No -> false
            | `Yes | `Unknown -> true
          then begin
            let stream =
              Body.create_stream
                Response.read_body_chunk
                (Response.make_body_reader res ic)
            in
            (* finalise could run in a thread different from the lwt main thread.
             * You may therefore not call into Lwt from a finaliser. *)
            let closed = ref false in
            Gc.finalise_last
              begin fun () ->
                if not !closed then
                  Log.warn (fun m ->
                      m
                        "Body not consumed, leaking stream! Refer to \
                         https://github.com/mirage/ocaml-cohttp/issues/730 for \
                         additional details")
              end
              stream;
            Lwt.wakeup_later res_r (res, Body.of_stream stream);
            Lwt_stream.closed stream >>= fun () ->
            closed := true;
            log connection "body finished";
            Queue.take connection.in_flight |> ignore;
            Lwt_condition.broadcast connection.condition ();
            reader connection
          end
          else begin
            Queue.take connection.in_flight |> ignore;
            Lwt_condition.broadcast connection.condition ();
            Lwt.wakeup_later res_r (res, `Empty);
            reader connection;
          end
        | `Eof ->
          log connection "read EOF";
          begin match connection.state with
          | Full (_, oc) | Closing (_, oc) ->
            Net.close_out oc;
            connection.state <- Closed
          | Half _ ->
            connection.state <- Closed
          (* Failed is no special case since we read proper EOF. *)
          | Closed | Failed _ -> ()
          | Connecting _ -> assert false
          end;
          connection.finalise connection >>= fun () ->
          queue_fail connection connection.in_flight Retry;
          Lwt.return_unit
        | `Invalid reason ->
          let e = Failure ("Cohttp_lwt failed to read response: " ^ reason) in
          connection.state <- Failed e;
          connection.finalise connection >>= fun () ->
          queue_fail connection connection.in_flight e;
          Lwt.return_unit

    let request connection ?(body = `Empty) req =
      match connection.state with
      | Connecting _ | Full _ ->
        log connection ("queueing request " ^ Request.resource req);
        let res, res_r = Lwt.wait () in
        Queue.push {req; body; res_r; res} connection.waiting;
        log connection "request queued";
        Lwt_condition.broadcast connection.condition ();
        log connection "broadcast sent";
        res
      | Closing _ | Half _ | Closed | Failed _ -> log connection "request rejected - Retry"; Lwt.fail Retry

    let rec writer connection =
      log connection "entering writer";
      match connection.state with
      | Full _ when Queue.is_empty connection.waiting ->
        Lwt.try_bind (fun () -> Lwt_condition.wait connection.condition)
          (fun _ -> writer connection)
          (fun _ -> writer connection)
      | Closing (ic, oc) when Queue.is_empty connection.waiting ->
        connection.state <- Half ic;
        Net.close_out oc;
        Lwt.return_unit
      | Full (ic, oc)
      | Closing (ic, oc) ->
        log connection "writer got something in queue";
        let {req; body; res_r; _} as work = Queue.take connection.waiting in
        Queue.push work connection.in_flight;
        Lwt_condition.broadcast connection.condition ();
        Lwt.catch
          begin fun () -> (* try *)
            log connection "writing";
            Request.write (fun writer ->
                Body.write_body (Request.write_body writer) body
              ) req oc
          end
          begin fun e -> (* with *)
            log connection "writing failed";
            (try Net.close_out oc with _ -> ());
            (* XXX empty waiting queue ? *)
            connection.state <- Half ic;
            Lwt.wakeup_later_exn res_r e;
            Lwt.return_unit
          end
        >>= fun () ->
        writer connection
      | Failed e ->
        queue_fail connection connection.waiting e;
        Lwt.return_unit
      | Half _ | Closed -> Lwt.return_unit
      | Connecting _ -> assert false

    let counter = ref 0
    let connect
        ?(finalise=fun _ -> Lwt.return_unit)
        ?(ctx=Net.default_ctx)
        uri
      =
      let channels =
        Net.connect_uri ~ctx uri >|= fun (_, ic, oc) ->
        (ic, oc)
      in
      let connection =
        { finalise
        ; in_flight = Queue.create ()
        ; waiting = Queue.create ()
        ; state = Connecting channels
        ; condition = Lwt_condition.create ()
        ; id = !counter
        }
      in
      incr counter;
      Lwt.on_any channels
        begin fun channels ->
          log connection "Connected";
          connection.state <- Full channels;
          Lwt.async (fun () -> reader connection);
          Lwt.async (fun () -> writer connection);
        end
        begin fun e ->
          log connection "connecting failed";
          connection.state <- Failed e
        end;
      log connection "created";
      connection
  end

  module Conn_cache =
  struct
    type t = < request : ?body:Body.t -> Request.t -> (Response.t * Body.t) Lwt.t >
    module Uri_map = Map.Make (Uri)

    let no_cache ?ctx:ctx () :t = object
      method request ?body req =
        let connection = Connection.connect ?ctx (Request.uri req) in
        let res = Connection.request connection ?body req in
        (* this can be simplified when https://github.com/mirage/ocaml-conduit/pull/319 is released. *)
        Lwt.async begin fun () ->
          res >>= fun (_, body) ->
          match body with
          | `Empty | `String _ | `Strings _ -> Lwt.return_unit
          | `Stream stream ->
            Lwt_stream.closed stream >>= fun () ->
            Connection.close connection;
            Lwt.return_unit
        end;
        res
    end

    (*
     * Select from existing connections by hostname, not IP.
     * One hostname may resolve to multiple IP addresses.
     * In such a case we want only one connection.
     * The downside is that some resolver redirections may break.
     *)

    let strip_uri uri =
      Uri.(make ?scheme:(scheme uri) ?host:(host uri) ?port:(port uri) ())

    let cache ?ctx:ctx ?(keep=60_000) ?(retry=2) ?(parallel=1) ?(depth=1) () =
      object(self)
        val mutable cache :Connection.t list Uri_map.t = Uri_map.empty
        val mutex = Lwt_mutex.create ()

        method private get_connection addr =
          let finalise connection =
            Lwt_mutex.with_lock mutex @@ fun () ->
            let conns = Uri_map.find addr cache in
            let conns = List.filter ((!=) connection) conns in
            if conns = []
            then cache <- cache |> Uri_map.remove addr
            else cache <- cache |> Uri_map.add addr conns;
            prerr_endline "removed from cache";
            Lwt.return_unit
          in
          let connect () =
            let connection = Connection.connect ~finalise ?ctx addr
            and timeout = ref Lwt.return_unit in
            let rec busy () =
              Lwt.cancel !timeout;
              if Connection.length connection = 0 then begin
                timeout :=
                  Time.sleep_ns Int64.(mul (of_int keep) 1_000_000L) >>= fun () ->
                  Connection.close connection; (* failure is ignored *)
                  Lwt.return_unit
              end;
              Lwt.on_termination (Connection.notify connection) busy
            in busy ();
            connection
          in
          Lwt_mutex.lock mutex >>= fun () ->
          match Uri_map.find_opt addr cache with
          | None ->
            let connection = connect () in
            cache <- cache |> Uri_map.add addr [connection];
            Lwt_mutex.unlock mutex;
            Lwt.return connection
          | Some conns ->
            let rec search length = function
              | a :: [] -> a, length + 1
              | a :: b :: tl
                when Connection.length a < Connection.length b ->
                search (length + 1) (a :: tl)
              | _ :: tl ->
                search (length + 1) tl
              | [] -> assert false
            in
            match search 0 conns with
            | shallowest, _ when Connection.length shallowest = 0 ->
              Lwt_mutex.unlock mutex;
              Lwt.return shallowest
            | _, length when length < parallel ->
              let connection = connect () in
              cache <- cache |> Uri_map.add addr (connection :: conns);
              Lwt_mutex.unlock mutex;
              Lwt.return connection
            | shallowest, _ when Connection.length shallowest < depth ->
              Lwt_mutex.unlock mutex;
              Lwt.return shallowest
            | _ ->
              Lwt_mutex.unlock mutex;
              Lwt.try_bind
                (fun () -> conns |> List.map Connection.notify |> Lwt.choose)
                (fun _ -> self#get_connection addr)
                (fun _ -> self#get_connection addr)

        method request ?body req =
          let addr = strip_uri (Request.uri req) in
          let rec request retry =
            self#get_connection addr >>= fun conn ->
            prerr_endline "cache: got connection";
            Lwt.catch (fun () -> Connection.request conn ?body req)
              begin function
              | Retry as e ->
                begin match body with
                | Some `Stream _ -> prerr_endline "cannot retry because of stream body"; Lwt.fail Retry
                | None | Some `Empty | Some `String _ | Some `Strings _ ->
                  Printf.eprintf "Got Retry. %u retries left\n" retry;
                  if retry <= 0
                  then Lwt.fail e
                  else request (retry - 1)
                end
              | e -> prerr_endline "Connection.request failed"; Lwt.fail e
              end
          in
          request retry
      end

    let default = ref (cache ())
    let set_default x = default := x

    let request ?(conn_cache :t option) ?body req =
      let conn_cache =
        match conn_cache with
        | None -> !default
        | Some x -> x
      in
      conn_cache#request ?body req
  end

  let is_meth_chunked = function
    | `HEAD -> false
    | `GET -> false
    | `DELETE -> false
    | _ -> true

  let call ?(ctx = Net.default_ctx) ?headers ?(body = `Empty) ?chunked meth uri
    =
    let headers = match headers with None -> Header.init () | Some h -> h in
    let chunked =
      match chunked with None -> is_meth_chunked meth | Some v -> v
    in
    begin
      if chunked
      then
        Lwt.return
          (Request.make_for_client ~headers ~chunked meth uri,
           body)
      else
        (* If chunked is not allowed, then obtain the body length and
           insert header *)
        Body.length body >|= fun (body_length, buf) ->
        (Request.make_for_client ~headers ~chunked ~body_length meth uri,
         buf)
    end >>= fun (req,body) ->
    let conn_cache = Conn_cache.no_cache ~ctx () in
    Conn_cache.request ~conn_cache ~body req

  (* The HEAD should not have a response body *)
  let head ?ctx ?headers uri = call ?ctx ?headers `HEAD uri >|= fst
  let get ?ctx ?headers uri = call ?ctx ?headers `GET uri

  let delete ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?headers ?body ?chunked `DELETE uri

  let post ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?headers ?body ?chunked `POST uri

  let put ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?headers ?body ?chunked `PUT uri

  let patch ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?headers ?body ?chunked `PATCH uri

  let post_form ?ctx ?headers ~params uri =
    let headers =
      Header.add_opt_unless_exists headers "content-type"
        "application/x-www-form-urlencoded"
    in
    let body = Body.of_string (Uri.encoded_of_query params) in
    post ?ctx ~chunked:false ~headers ~body uri

  let callv ?ctx uri reqs =
    let connection = Connection.connect ?ctx uri
    and mutex = Lwt_mutex.create () in
    Lwt.return @@ Lwt_stream.from @@ fun () ->
    Lwt_stream.get reqs >>= function
    | None ->
      Connection.close connection |> ignore;
      Lwt.return_none
    | Some (req, body) ->
      Lwt_mutex.with_lock mutex @@ fun () ->
      Connection.request connection ~body req >|= Option.some
end
