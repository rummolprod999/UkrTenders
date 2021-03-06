namespace Ukr

open MySql.Data.MySqlClient
open System
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
    
    let DownloadString(url : string) =
        let mutable s = null
        let count = ref 0
        let mutable continueLooping = true
        while continueLooping do
            try 
                //let t ():string = (new TimedWebClient()).DownloadString(url: Uri)
                let task = Task.Run(fun () -> (new TimedWebClient()).DownloadString(url : string))
                if task.Wait(TimeSpan.FromSeconds(100.)) then 
                    s <- task.Result
                    continueLooping <- false
                else raise <| TimeoutException()
            with _ -> 
                if !count >= 10 then 
                    Logging.Log.logger (sprintf "Не удалось скачать %s xml за %d попыток" url !count)
                    continueLooping <- false
                else incr count
                Thread.Sleep(5000)
        s
    
    let DownloadStringMysql (url : string) (con : MySqlConnection) =
        let mutable s = null
        let count = ref 0
        let mutable continueLooping = true
        while continueLooping do
            try 
                //let t ():string = (new TimedWebClient()).DownloadString(url: Uri)
                let task = Task.Run(fun () -> (new TimedWebClient()).DownloadString(url : string))
                if task.Wait(TimeSpan.FromSeconds(100.)) then 
                    s <- task.Result
                    continueLooping <- false
                else raise <| TimeoutException()
            with _ -> 
                con.Ping() |> ignore
                if !count >= 30 then 
                    Logging.Log.logger (sprintf "Не удалось скачать %s xml за %d попыток" url !count)
                    continueLooping <- false
                else incr count
                Thread.Sleep(5000)
        s
