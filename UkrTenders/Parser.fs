namespace Ukr

open System
open System.IO

module Parser = 
    let Parser() = 
        Logging.Log.logger ("Начало парсинга")
        try
            Parsing.Parsing()
        with
        | _ as ex -> Logging.Log.logger(ex.Message) 
        Logging.Log.logger ("Конец парсинга")
    
    let init (s : Setting.T) = 
        if String.IsNullOrEmpty(s.TempPathTenders) || String.IsNullOrEmpty(s.LogPathTenders) then 
            printf "Не получится создать папки для парсинга"
            Environment.Exit(0)
        else 
            match Directory.Exists(s.TempPathTenders) with
            | true -> 
                let dirInfo = new DirectoryInfo(s.TempPathTenders)
                dirInfo.Delete(true)
                Directory.CreateDirectory(s.TempPathTenders) |> ignore
            | false -> Directory.CreateDirectory(s.TempPathTenders) |> ignore
            match Directory.Exists(s.LogPathTenders) with
            | false -> Directory.CreateDirectory(s.LogPathTenders) |> ignore
            | true -> ()
        Logging.FileLog <- sprintf "%s%clog_parsing_ukr_%s.log" s.LogPathTenders Path.DirectorySeparatorChar 
                           <| DateTime.Now.ToString("dd_MM_yyyy")
    
    let Parsing() : unit = 
        let Set = Setting.getSettings()
        init (Set)
        Parser()
