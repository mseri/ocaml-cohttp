module Make_no_cache (Connection : S.Connection) : sig
  include S.Connection_cache with module IO = Connection.IO

  val create : ?ctx:Connection.ctx -> unit -> t
end
=
struct
  module IO = Connection.IO
  open IO

  type t = ?body:Body.t -> Cohttp.Request.t -> (Cohttp.Response.t * Body.t) IO.t

  let request = Fun.id

  let create ?ctx () ?body req =
    let connection =
      Connection.connect ?ctx ~persistent:false (Cohttp.Request.uri req)
    in
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

module Make (Connection : S.Connection) (Sleep : S.Sleep) : sig
  include S.Connection_cache with module IO = Connection.IO

  type ctx = Connection.ctx

  val create :
    ?ctx:ctx ->
    ?keep:int64 ->
    ?retry:int ->
    ?parallel:int ->
    ?depth:int ->
    unit -> t
end
=
struct
  module IO = Connection.IO
  open IO

  type ctx = Connection.ctx

  exception Retry = S.Retry

  module Uri_map = Map.Make (Uri)

  (* type t = < request : ?body:Body.t -> Cohttp.Request.t -> (Cohttp.Response.t * Body.t) IO.t > *)
  type t =
    { mutable cache : Connection.t list Uri_map.t
    ; ctx : ctx option
    ; keep : int64
    ; retry : int
    ; parallel : int
    ; depth : int
    }

  (*
   * Select from existing connections by hostname, not IP.
   * One hostname may resolve to multiple IP addresses.
   * In such a case we want only one connection.
   * The downside is that some resolver redirections may break.
   *)

  let create ?ctx ?(keep=60_000_000_000L) ?(retry=2) ?(parallel=4) ?(depth=100) () =
    { cache = Uri_map.empty
    ; ctx ; keep ; retry ; parallel ; depth
    }

  let rec get_connection self addr =
    let finalise connection =
      let conns = Uri_map.find addr self.cache in
      let conns = List.filter ((!=) connection) conns in
      if conns = []
      then self.cache <- self.cache |> Uri_map.remove addr
      else self.cache <- self.cache |> Uri_map.add addr conns;
      Lwt.return_unit
    in
    let connect () =
      let connection = Connection.connect ~finalise ?ctx:self.ctx addr
      and timeout = ref Lwt.return_unit in
      let rec busy () =
        Lwt.cancel !timeout;
        if Connection.length connection = 0 then begin
          timeout :=
            Sleep.sleep_ns self.keep >>= fun () ->
            Connection.close connection; (* failure is ignored *)
            Lwt.return_unit
        end;
        Lwt.on_termination (Connection.notify connection) busy
      in busy ();
      connection
    in
    match Uri_map.find_opt addr self.cache with
    | None ->
      let connection = connect () in
      self.cache <- self.cache |> Uri_map.add addr [connection];
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
        Lwt.return shallowest
      | _, length when length < self.parallel ->
        let connection = connect () in
        self.cache <- self.cache |> Uri_map.add addr (connection :: conns);
        Lwt.return connection
      | shallowest, _ when Connection.length shallowest < self.depth ->
        Lwt.return shallowest
      | _ ->
        Lwt.try_bind
          (fun () -> conns |> List.map Connection.notify |> Lwt.choose)
          (fun _ -> get_connection self addr)
          (fun _ -> get_connection self addr)

  let strip_uri uri =
    Uri.(make ?scheme:(scheme uri) ?host:(host uri) ?port:(port uri) ())

  let request self ?body req =
    let addr = strip_uri (Cohttp.Request.uri req) in
    let rec request retry =
      get_connection self addr >>= fun conn ->
      Lwt.catch (fun () -> Connection.request conn ?body req)
        begin function
        | Retry as e ->
          begin match body with
          | Some `Stream _ -> Lwt.fail Retry
          | None | Some `Empty | Some `String _ | Some `Strings _ ->
            if retry <= 0
            then Lwt.fail e
            else request (retry - 1)
          end
        | e -> Lwt.fail e
        end
    in request self.retry
end
