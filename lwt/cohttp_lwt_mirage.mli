(*
 * Copyright (c) 2012 Anil Madhavapeddy <anil@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)

(** HTTP client and server using the [Lwt_unix] interfaces. *)

(** The [Request] module holds the information about a HTTP request, and
    also includes the {! Cohttp_lwt_mirage_io} functions to handle large 
    message bodies. *)
module Request : sig
  include module type of Cohttp.Request with type t = Cohttp.Request.t
  include Cohttp.Request.S with module IO=Cohttp_lwt_mirage_io
end

(** The [Response] module holds the information about a HTTP request, and
    also includes the {! Cohttp_lwt_mirage_io} functions to handle large 
    message bodies. *)
module Response : sig
  include module type of Cohttp.Response with type t = Cohttp.Response.t
  include Cohttp.Response.S with module IO=Cohttp_lwt_mirage_io
end

(** The [Client] module implements an HTTP client interface. *)
module Client : 
  Cohttp_lwt.Client with module IO=Cohttp_lwt_mirage_io

(** The Mirage [S] module type defines the additional Mirage-specific
    functions that are exposed by the {! Cohttp_lwt.Server} interface. 
    This is primarily the {! listen} function to actually create the
    server instance and response to incoming requests. *)
module type S = sig
  module Server : Cohttp_lwt.Server

  val listen :
    ?timeout:float ->
    Net.Manager.t -> Net.Nettypes.ipv4_src -> Server.config -> unit Lwt.t
end

(** The [Server] module implement the full Mirage HTTP server interface,
  including the Mirage-specific functions defined in {! S }. *)
module Server : sig
  include Cohttp_lwt.Server with module IO=Cohttp_lwt_mirage_io
  include S with type Server.config = config
end 
