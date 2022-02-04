module type S = sig
  type t
  type body
  type 'a promise

  val set_default : t -> unit

  val request : ?conn_cache:t -> ?body:body -> Request.t ->
    (Response.t * body) promise
end
