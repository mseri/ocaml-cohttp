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

  module type PipelineS = sig
    type t

    val create :
      ?finalise:(t -> unit Lwt.t) ->
      ?ctx:ctx ->
      Uri.t -> (* XXX this should actually be something like Conduit.endp *)
      t
    val length : t -> int
    val notify : t -> unit Lwt.t
    val shutdown : t -> unit Lwt.t
    val process : t -> ?body:Body.t -> Cohttp.Request.t ->
      (Cohttp.Response.t * Body.t) Lwt.t
  end

  module Pipeline :PipelineS = struct
    type connection =
      | Connecting of (Net.IO.ic * Net.IO.oc) Lwt.t
      | Full of (Net.IO.ic * Net.IO.oc)
      | Half of Net.IO.ic
      | Closed
      | Failed of exn
    type req_resr =
      { req :Request.t
      ; body :Body.t
      ; res_r :(Response.t * Body.t) Lwt.u
      ; res :(Response.t * Body.t) Lwt.t }
    type t =
      { mutable conn :connection
      ; in_flight :req_resr Queue.t
      ; condition :unit Lwt_condition.t (* watching in_flight *)
      ; finalise :(t -> unit Lwt.t)
      ; mutex :Lwt_mutex.t
      ; id :int
      }

    let length pipeline =
      Queue.length pipeline.in_flight

    let notify pipeline =
      Lwt_condition.wait pipeline.condition

    let to_string pipeline =
      Printf.sprintf "Pipeline %u (%u %s)"
        pipeline.id
        (length pipeline)
        (match pipeline.conn with
        |Connecting _ -> "Connecting"
        |Full _ -> "Full"
        |Half _ -> "Half"
        |Closed -> "Closed"
        |Failed _ -> "Failed")

    let log pipeline s = prerr_endline (to_string pipeline ^ ": " ^ s)

    let finalise pipeline =
      (* be sure to call user (in wakeup_later_exn) only after finalise had a
       * chance to remove pipeline from connection cache. Otherwise user might
       * try to requeue request when connection cache still holds failed
       * pipeline. *)
      log pipeline "finalising";
      match pipeline.conn with
      | Connecting _ | Full _ | Half _ -> assert false
      | Closed ->
        pipeline.finalise pipeline >>= fun () ->
        assert (Queue.is_empty pipeline.in_flight);
        Lwt.return_unit
      | Failed e ->
        pipeline.finalise pipeline >>= fun () ->
        if not (Queue.is_empty pipeline.in_flight) then begin
          Queue.iter
            (fun {res_r; _} -> Lwt.wakeup_later_exn res_r e)
            pipeline.in_flight;
          Queue.clear pipeline.in_flight;
          Lwt_condition.broadcast_exn pipeline.condition e;
        end;
        Lwt.return_unit

    let rec reader pipeline =
      let ic = match pipeline.conn with
        | Connecting _ | Closed | Failed _ -> assert false
        | Full (ic, _) | Half ic -> ic
      in
      log pipeline "reader waiting";
      Response.read ic >>= fun res ->
      match res with
      | `Ok res ->
        log pipeline "got response";
        (* don't take from queue yet, because body may still be in flight *)
        let {req; res_r; _} = Queue.peek pipeline.in_flight in

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
          log pipeline "body finished";
          Queue.take pipeline.in_flight |> ignore;
          Lwt_condition.broadcast pipeline.condition ();
          reader pipeline
        end
        else begin
          Queue.take pipeline.in_flight |> ignore;
          Lwt_condition.broadcast pipeline.condition ();
          Lwt.wakeup_later res_r (res, `Empty);
          reader pipeline;
        end
      | `Eof ->
        log pipeline "read EOF";
        begin match pipeline.conn with
          | Full _ | Half _ when Queue.is_empty pipeline.in_flight ->
              pipeline.conn <- Closed
          | Full (_, oc) ->
              Net.close_out oc;
              pipeline.conn <- Failed Retry
          | Half _ ->
              pipeline.conn <- Failed Retry
          | Failed _ -> ()
          | Connecting _ | Closed -> assert false
        end;
        finalise pipeline
      | `Invalid reason ->
        let e = Failure ("Cohttp_lwt failed to read response: " ^ reason) in
        pipeline.conn <- Failed e;
        finalise pipeline

    (* in shutdown and process I rely on mutex waiters receiving the lock in the
     * order they requested it, which is the case in Lwt. *)
    let shutdown pipeline =
      Lwt_mutex.with_lock pipeline.mutex @@ fun () ->
      log pipeline "shutdown";
      match pipeline.conn with
        | Full (ic, oc) ->
          pipeline.conn <- Half ic;
          Net.close_out oc;
          Lwt.return_unit
        | Half _ | Closed -> Lwt.return_unit
        | Failed e -> Lwt.fail e
        | Connecting _ -> assert false

    let process pipeline ?(body = `Empty) req =
      let res, res_r = Lwt.wait () in
      (* give reader a chance to read possible EOF. *)
      Lwt.pause () >>= Lwt.pause >>= Lwt.pause >>= fun () ->
      Lwt_mutex.with_lock pipeline.mutex begin fun () ->
        match pipeline.conn with
        | Full (_, oc) ->
          log pipeline ("writing request " ^ (Request.uri req |> Uri.to_string));
          Queue.push {req; body; res_r; res} pipeline.in_flight;
          Lwt_condition.broadcast pipeline.condition ();
          Lwt.catch
            begin fun () -> (* try *)
                Request.write (fun writer ->
                    Body.write_body (Request.write_body writer) body
                  ) req oc
            end
            begin fun e -> (* with *)
              (try Net.close_out oc with _ -> ());
              Lwt.wakeup_later_exn res_r e;
              Lwt.return_unit
            end
        | Half _ | Closed ->
            Lwt.wakeup_later_exn res_r Retry;
            Lwt.return_unit
        | Failed e ->
            Lwt.wakeup_later_exn res_r e;
            Lwt.return_unit
        | Connecting _ -> assert false
        end >>= fun () ->
      res

    let counter = ref 0
    let create
      ?(finalise=fun _ -> Lwt.return_unit)
      ?(ctx=Net.default_ctx)
      uri
      =
      incr counter;
      let mutex = Lwt_mutex.create () in
      Lwt.async (fun () -> Lwt_mutex.lock mutex);
      let connection =
        Net.connect_uri ~ctx uri >|= fun (_, ic, oc) ->
        (ic, oc)
      in
      let pipeline =
        { finalise
        ; in_flight = Queue.create ()
        ; conn = Connecting connection
        ; mutex
        ; condition = Lwt_condition.create ()
        ; id = !counter
        }
      in
      Lwt.on_any connection
        begin fun connection ->
          pipeline.conn <- Full connection;
          Lwt_mutex.unlock pipeline.mutex;
          Lwt.async (fun () -> reader pipeline);
        end
        begin fun e ->
          pipeline.conn <- Failed e;
          Lwt_mutex.unlock pipeline.mutex;
        end;
      log pipeline "created";
      pipeline
  end

  module Conn_cache =
  struct
    type t = < process : ?body:Body.t -> Request.t -> (Response.t * Body.t) Lwt.t >
    module Uri_map = Map.Make (Uri)

    let no_cache ?ctx:ctx () :t = object
      method process ?body req =
        let pipeline = Pipeline.create ?ctx (Request.uri req) in
        let res = Pipeline.process pipeline ?body req in
        (* this can be simplified when https://github.com/mirage/ocaml-conduit/pull/319 is released. *)
        Lwt.async begin fun () ->
          res >>= fun (_, body) ->
          match body with
          | `Empty | `String _ | `Strings _ -> Lwt.return_unit
          | `Stream stream ->
              Lwt_stream.closed stream >>= fun () ->
              prerr_endline "no_cache shutting down pipeline";
              Pipeline.shutdown pipeline
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

    let cache ?ctx:ctx ?(keep=60_000) ?(retry=2) ?(parallel=4) ?(depth=1) () =
    object(self)
      val mutable cache :Pipeline.t list Uri_map.t = Uri_map.empty
      val mutex = Lwt_mutex.create ()

      method private get_pipeline addr =
        let finalise pipeline =
          Lwt_mutex.with_lock mutex @@ fun () ->
          let pipes = Uri_map.find addr cache in
          let pipes = List.filter ((!=) pipeline) pipes in
          if pipes = []
          then cache <- cache |> Uri_map.remove addr
          else cache <- cache |> Uri_map.add addr pipes;
          prerr_endline "  removed pipeline from cache";
          Lwt.return_unit
        in
        let create () =
          let pipeline = Pipeline.create ~finalise ?ctx addr
          and timeout = ref Lwt.return_unit in
          let rec busy () =
            Lwt.cancel !timeout;
            if Pipeline.length pipeline = 0 then begin
              timeout :=
                Time.sleep_ns Int64.(mul (of_int keep) 1_000_000L) >>= fun () ->
                prerr_endline "Pipeline timed out";
                Pipeline.shutdown pipeline (* failure is ignored *)
            end;
            Lwt.on_success (Pipeline.notify pipeline) busy
          in busy ();
          pipeline
        in
        Lwt_mutex.lock mutex >>= fun () ->
        match Uri_map.find_opt addr cache with
        | None ->
            let pipeline = create () in
            cache <- cache |> Uri_map.add addr [pipeline];
            Lwt_mutex.unlock mutex;
            Lwt.return pipeline
        | Some pipes ->
            let rec search length = function
              | a :: [] -> a, length + 1
              | a :: b :: tl
                when Pipeline.length a < Pipeline.length b ->
                search (length + 1) (a :: tl)
              | _ :: tl ->
                search (length + 1) tl
              | [] -> assert false
            in
            match search 0 pipes with
            | shallowest, _ when Pipeline.length shallowest = 0 ->
              Lwt_mutex.unlock mutex;
              Lwt.return shallowest
            | _, length when length < parallel ->
              let pipeline = create () in
              cache <- cache |> Uri_map.add addr (pipeline :: pipes);
              Lwt_mutex.unlock mutex;
              Lwt.return pipeline
            | shallowest, _ when Pipeline.length shallowest < depth ->
              Lwt_mutex.unlock mutex;
              Lwt.return shallowest
            | _ ->
              Lwt_mutex.unlock mutex;
              Lwt.try_bind
                (fun () -> pipes |> List.map Pipeline.notify |> Lwt.choose)
                (fun _ -> self#get_pipeline addr)
                (fun _ -> self#get_pipeline addr)

      method process ?body req =
        let addr = strip_uri (Request.uri req) in
        let rec process retry =
          self#get_pipeline addr >>= fun pipe ->
          Lwt.catch (fun () -> Pipeline.process pipe ?body req)
            begin function
            | Retry as e ->
                begin match body with
                | Some `Stream _ -> Lwt.fail Retry
                | None | Some `Empty | Some `String _ | Some `Strings _ ->
                    Printf.eprintf "Got Retry %u retries left\n" retry;
                    if retry <= 0
                    then Lwt.fail e
                    else process (retry - 1)
                end
            | e -> Lwt.fail e
            end
        in
        process retry
    end

    let default = ref (cache ())
    let set_default x = default := x

    let process ?(conn_cache :t option) ?body req =
      let conn_cache =
        match conn_cache with
        | None -> !default
        | Some x -> x
      in
      conn_cache#process ?body req
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
    Conn_cache.process ~conn_cache ~body req

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
    let pipeline = Pipeline.create ?ctx uri
    and mutex = Lwt_mutex.create () in
    Lwt.return @@ Lwt_stream.from @@ fun () ->
      Lwt_stream.get reqs >>= function
      | None ->
        Pipeline.shutdown pipeline |> ignore;
        Lwt.return_none
      | Some (req, body) ->
        Lwt_mutex.with_lock mutex @@ fun () ->
        Pipeline.process pipeline ~body req >|= Option.some
end
