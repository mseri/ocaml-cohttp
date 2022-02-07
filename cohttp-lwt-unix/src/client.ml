module Sleep = struct
  let sleep_ns ns =
    Lwt_unix.sleep @@
    let open Int64 in
    if ns >= zero
    then to_float ns /. 1_000_000_000
    else (shift_right_logical ns 1 |> to_float) *. 2. /. 1_000_000_000.
end

include Cohttp_lwt.Make_client (Io) (Net) (Sleep)

let custom_ctx = Net.init
