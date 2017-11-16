namespace Ukr

open System
open System.Globalization
open System.IO
open System.Text

module Logging = 
    let mutable FileLog = ""
    
    type Log() = 
        static member logger ([<ParamArray>] args : Object []) = 
            let mutable s = ""
            s <- DateTime.Now.ToString()
            args |> Seq.iter (fun x -> (s <- x.ToString() |> sprintf "%s %s" s))
            (*for arg in args do
                   s <-  arg.ToString() |>  sprintf "%s %s" s*)
            use sw = new StreamWriter(FileLog, true, Encoding.Default)
            sw.WriteLine(s)
