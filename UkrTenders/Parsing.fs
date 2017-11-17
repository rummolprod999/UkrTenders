namespace Ukr

open MySql.Data.MySqlClient
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System
open System.Collections.Generic
open System.Data
open System.Xml

module Parsing = 
    let testint (t : JToken) : int = 
        match t with
        | null -> 0
        | _ -> (int) t
    
    let teststring (t : JToken) : string = 
        match t with
        | null -> ""
        | _ -> ((string) t).Trim()
    
    let testdate (t : string) : DateTime = 
        match t with
        | null | "null" -> DateTime.MinValue
        | _ -> DateTime.Parse(((string) t).Trim('"'))
    
    type Imperative<'T> = unit -> option<'T>
    
    type ImperativeBuilder() = 
        
        member x.Combine(a, b) = 
            (fun () -> 
            match a() with
            | Some(v) -> Some(v)
            | _ -> b())
        
        member x.Delay(f : unit -> Imperative<_>) : Imperative<_> = (fun () -> f () ())
        member x.Return(v) : Imperative<_> = (fun () -> Some(v))
        member x.Zero() = (fun () -> None)
        member x.Run(imp) = 
            match imp() with
            | Some(v) -> v
            | _ -> failwith "Nothing returned!"
    
    let ParserT (d : JToken) (stn : Setting.T) (con : MySqlConnection) : unit = 
        con.Open()
        let id = teststring <| d.SelectToken("id")
        let dateModified = testdate <| JsonConvert.SerializeObject(d.SelectToken("dateModified"))
        let selectTend = 
            sprintf 
                "SELECT id_tender FROM %stender WHERE id_xml = @id_xml AND date_version = @date_version AND type_fz = 5" 
                stn.Prefix
        let cmd : MySqlCommand = new MySqlCommand(selectTend, con)
        cmd.Prepare()
        cmd.Parameters.AddWithValue("@id_xml", id) |> ignore
        cmd.Parameters.AddWithValue("@date_version", dateModified) |> ignore
        let reader : MySqlDataReader = cmd.ExecuteReader()
        if reader.HasRows then reader.Close()
        else 
            reader.Close()
            let mutable cancelStatus = 0
            let selectDateT = 
                sprintf "SELECT id_tender, date_version, cancel FROM %stender WHERE id_xml = @id_xml AND type_fz = 5" 
                    stn.Prefix
            let cmd2 = new MySqlCommand(selectDateT, con)
            cmd2.Prepare()
            cmd2.Parameters.AddWithValue("@id_xml", id) |> ignore
            let adapter = new MySqlDataAdapter()
            adapter.SelectCommand <- cmd2
            let dt = new DataTable()
            adapter.Fill(dt) |> ignore
            for row in dt.Rows do
                //printfn "%A" <| (row.["date_version"])
                match dateModified >= ((row.["date_version"]) :?> DateTime) with
                | true -> row.["cancel"] <- 1
                | false -> cancelStatus <- 1
            let commandBuilder = new MySqlCommandBuilder(adapter)
            commandBuilder.ConflictOption <- ConflictOption.OverwriteChanges
            adapter.Update(dt) |> ignore
            let ten = Download.DownloadString <| sprintf "http://public.api.openprocurement.org/api/2.4/tenders/%s" id
            let jsn = JObject.Parse(ten)
            (*let enquiryPeriodstartDateS = (string) <| JsonConvert.SerializeObject(jsn.SelectToken("data.enquiryPeriod.startDate"))
            printfn "%A %d" enquiryPeriodstartDateS enquiryPeriodstartDateS.Length*)
            let enquiryPeriodstartDate = 
                testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.enquiryPeriod.startDate"))
            printfn "%A" enquiryPeriodstartDate
        ()
    
    let parserL (o : JObject) (stn : Setting.T) (connectstring : string) : unit = 
        let uri = teststring <| o.SelectToken("next_page.uri")
        //printfn "%A" uri
        let tdata = o.SelectToken("data") :?> JArray
        for d in tdata do
            try 
                using (new MySqlConnection(connectstring)) (ParserT d stn)
            with ex -> Logging.Log.logger ex
    
    let ParsungListTenders (j : JObject) (sett : Setting.T) = 
        let connectstring = 
            sprintf 
                "Server=%s;port=%d;Database=%s;User Id=%s;password=%s;CharSet=utf8;Convert Zero Datetime=True;default command timeout=3600;Connection Timeout=3600" 
                sett.Server sett.Port sett.Database sett.UserDb sett.PassDb
        try 
            parserL j sett connectstring
        with x -> Logging.Log.logger x
    
    let Parsing(st : Setting.T) = 
        //let s = Download.DownloadString("https://stackoverflow.com/questions/15212133/increment-value-in-f")
        let startUrl = 
            sprintf "http://public.api.openprocurement.org/api/2.4/tenders?offset=%s" 
            <| DateTime.Now.AddHours(-30.).ToString("s")
        let mutable continueLooping = true
        let mutable urlDown = startUrl
        while continueLooping do
            match Download.DownloadString(urlDown) with
            | null -> ()
            | s -> 
                let json = JObject.Parse(s)
                
                let tpl = 
                    let uri = ((string) (json.SelectToken("next_page.uri")))
                    //printf "%A" uri
                    let count = ((json.SelectToken("data")) :?> JArray).Count
                    (uri, count)
                match tpl with
                | (null, _) -> continueLooping <- false
                | (x, cnt) when cnt < 100 -> 
                    ParsungListTenders json st
                    continueLooping <- false
                | (x, cnt) -> 
                    ParsungListTenders json st
                    urlDown <- x
