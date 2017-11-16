namespace Ukr

open System
open System.Globalization
open System.IO
open System.Net
open System.Text
open System.Threading
open System.Threading.Tasks

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

module Download = 
    type TimedWebClient() = 
        inherit WebClient()
        override this.GetWebRequest(address : Uri) = 
            let wr = base.GetWebRequest(address)
            wr.Timeout <- 600000
            wr
    
    let DownloadString url = 
        let mutable s = ""
        let count = ref 0
        let mutable continueLooping = true
        while continueLooping do
            try 
                //let t ():string = (new TimedWebClient()).DownloadString(url: Uri)
                let task = Task.Run(fun () -> (new TimedWebClient()).DownloadString(url : string))
                if task.Wait(TimeSpan.FromSeconds(650.)) then 
                    s <- task.Result
                    continueLooping <- false
                else raise <| new TimeoutException()
            with _ -> 
                if !count >= 100 then 
                    Logging.Log.logger (sprintf "Не удалось скачать xml за %d попыток" !count)
                    continueLooping <- false
                else incr count
        s
