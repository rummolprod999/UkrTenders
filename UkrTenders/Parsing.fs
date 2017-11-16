namespace Ukr

open System
module Parsing =
    type Imperative<'T> = unit -> option<'T>
    
    type ImperativeBuilder() = 
      member x.Combine(a, b) = (fun () ->
        match a() with 
        | Some(v) -> Some(v) 
        | _ -> b() )
      member x.Delay(f:unit -> Imperative<_>) : Imperative<_> = (fun () -> f()())
      member x.Return(v) : Imperative<_> = (fun () -> Some(v))
      member x.Zero() = (fun () -> None)
      member x.Run(imp) = 
        match imp() with
        | Some(v) -> v
        | _ -> failwith "Nothing returned!"
    let Parsing() = 
        //let s = Download.DownloadString("https://stackoverflow.com/questions/15212133/increment-value-in-f")
        let startUrl = sprintf "http://public.api.openprocurement.org/api/2.4/tenders?offset=%s" <| DateTime.Now.AddHours(-30.).ToString("s")
        printf "%s" startUrl
        