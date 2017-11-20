namespace Ukr

open MySql.Data.MySqlClient
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System
open System.Collections.Generic
open System.Data
open System.Xml

module Parsing = 
    let tenderCount = ref 0
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
            let tenurl = sprintf "http://public.api.openprocurement.org/api/2.4/tenders/%s" id
            let ten = Download.DownloadString tenurl
            let jsn = JObject.Parse(ten)
            (*let enquiryPeriodstartDateS = (string) <| JsonConvert.SerializeObject(jsn.SelectToken("data.enquiryPeriod.startDate"))
            printfn "%A %d" enquiryPeriodstartDateS enquiryPeriodstartDateS.Length*)
            let enquiryPeriodstartDate = 
                testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.enquiryPeriod.startDate"))
            //printfn "%A" enquiryPeriodstartDate
            let tenderID = teststring <| jsn.SelectToken("data.tenderID")
            let href = sprintf "https://prozorro.gov.ua/tender/%s" tenderID
            let description = teststring <| jsn.SelectToken("data.description")
            let PurchaseObjectInfo = description |> sprintf "%s %s" (teststring <| (jsn.SelectToken("data.title")))
            let NoticeVersion = ""
            let Printform = href
            let identifierId = teststring <| jsn.SelectToken("data.procuringEntity.identifier.id")
            let IdOrg = ref 0
            if identifierId <> "" then 
                let selectOrg = sprintf "SELECT id_organizer FROM %sorganizer WHERE inn = @inn" stn.Prefix
                let cmd3 = new MySqlCommand(selectOrg, con)
                cmd3.Prepare()
                cmd3.Parameters.AddWithValue("@inn", identifierId) |> ignore
                let reader = cmd3.ExecuteReader()
                match reader.HasRows with
                | true -> 
                    reader.Read() |> ignore
                    IdOrg := reader.GetInt32("id_organizer")
                    reader.Close()
                | false -> 
                    reader.Close()
                    let legalName = teststring <| jsn.SelectToken("data.procuringEntity.identifier.legalName")
                    let telephone = teststring <| jsn.SelectToken("data.procuringEntity.contactPoint.telephone")
                    let name = teststring <| jsn.SelectToken("data.procuringEntity.contactPoint.name")
                    let email = teststring <| jsn.SelectToken("data.procuringEntity.contactPoint.email")
                    let postalCode = teststring <| jsn.SelectToken("data.procuringEntity.address.postalCode")
                    let countryName = teststring <| jsn.SelectToken("data.procuringEntity.address.countryName")
                    let streetAddress = teststring <| jsn.SelectToken("data.procuringEntity.address.streetAddress")
                    let locality = teststring <| jsn.SelectToken("data.procuringEntity.address.locality")
                    let region = teststring <| jsn.SelectToken("data.procuringEntity.address.region")
                    let factAddress = 
                        (sprintf "%s %s %s %s %s" postalCode countryName region locality streetAddress).Trim()
                    let addOrganizer = 
                        sprintf 
                            "INSERT INTO %sorganizer SET full_name = @full_name, post_address = @post_address, fact_address = @fact_address, inn = @inn, contact_person = @contact_person, contact_email = @contact_email, contact_phone = @contact_phone" 
                            stn.Prefix
                    let cmd5 = new MySqlCommand(addOrganizer, con)
                    cmd5.Parameters.AddWithValue("@full_name", legalName) |> ignore
                    cmd5.Parameters.AddWithValue("@post_address", factAddress) |> ignore
                    cmd5.Parameters.AddWithValue("@fact_address", factAddress) |> ignore
                    cmd5.Parameters.AddWithValue("@inn", identifierId) |> ignore
                    cmd5.Parameters.AddWithValue("@contact_person", name) |> ignore
                    cmd5.Parameters.AddWithValue("@contact_email", email) |> ignore
                    cmd5.Parameters.AddWithValue("@contact_phone", telephone) |> ignore
                    cmd5.ExecuteNonQuery() |> ignore
                    IdOrg := int cmd5.LastInsertedId
            let idPlacingWay = ref 0
            let placingWayName = teststring <| jsn.SelectToken("data.submissionMethod")
            if placingWayName <> "" then 
                let selectPlacingWay = sprintf "SELECT id_placing_way FROM %splacing_way WHERE name= @name" stn.Prefix
                let cmd6 = new MySqlCommand(selectPlacingWay, con)
                cmd6.Prepare()
                cmd6.Parameters.AddWithValue("@name", placingWayName) |> ignore
                let reader3 = cmd6.ExecuteReader()
                match reader3.HasRows with
                | true -> 
                    reader3.Read() |> ignore
                    idPlacingWay := reader3.GetInt32("id_placing_way")
                    reader3.Close()
                | false -> 
                    reader3.Close()
                    let insertPlacingWay = sprintf "INSERT INTO %splacing_way SET name= @name" stn.Prefix
                    let cmd7 = new MySqlCommand(insertPlacingWay, con)
                    cmd7.Prepare()
                    cmd7.Parameters.AddWithValue("@name", placingWayName) |> ignore
                    cmd7.ExecuteNonQuery() |> ignore
                    idPlacingWay := int cmd7.LastInsertedId
            let idEtp = ref 0
            let etpName = "ProZorro"
            let etpUrl = "https://prozorro.gov.ua"
            if etpName <> "" then 
                let selectEtp = sprintf "SELECT id_etp FROM %setp WHERE name = @name AND url = @url" stn.Prefix
                let cmd6 = new MySqlCommand(selectEtp, con)
                cmd6.Prepare()
                cmd6.Parameters.AddWithValue("@name", etpName) |> ignore
                cmd6.Parameters.AddWithValue("@url", etpUrl) |> ignore
                let reader3 = cmd6.ExecuteReader()
                match reader3.HasRows with
                | true -> 
                    reader3.Read() |> ignore
                    idEtp := reader3.GetInt32("id_etp")
                    reader3.Close()
                | false -> 
                    reader3.Close()
                    let insertEtp = sprintf "INSERT INTO %setp SET name= @name, url= @url, conf=0" stn.Prefix
                    let cmd7 = new MySqlCommand(insertEtp, con)
                    cmd7.Prepare()
                    cmd7.Parameters.AddWithValue("@name", etpName) |> ignore
                    cmd7.Parameters.AddWithValue("@url", etpUrl) |> ignore
                    cmd7.ExecuteNonQuery() |> ignore
                    idEtp := int cmd7.LastInsertedId
            let startDate = testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.tenderPeriod.startDate"))
            let endDate = testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.tenderPeriod.endDate"))
            let biddingDate = testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.auctionPeriod.startDate"))
            let scoringDate = DateTime.MinValue
            let numVersion = 0
            let idTender = ref 0
            let insertTender = 
                sprintf 
                    "INSERT INTO %stender SET id_xml = @id_xml, purchase_number = @purchase_number, doc_publish_date = @doc_publish_date, href = @href, purchase_object_info = @purchase_object_info, type_fz = @type_fz, id_organizer = @id_organizer, id_placing_way = @id_placing_way, id_etp = @id_etp, end_date = @end_date, scoring_date = @scoring_date, bidding_date = @bidding_date, cancel = @cancel, date_version = @date_version, num_version = @num_version, notice_version = @notice_version, xml = @xml, print_form = @print_form" 
                    stn.Prefix
            let cmd9 = new MySqlCommand(insertTender, con)
            cmd9.Prepare()
            cmd9.Parameters.AddWithValue("@id_xml", id) |> ignore
            cmd9.Parameters.AddWithValue("@purchase_number", tenderID) |> ignore
            cmd9.Parameters.AddWithValue("@doc_publish_date", startDate) |> ignore
            cmd9.Parameters.AddWithValue("@href", href) |> ignore
            cmd9.Parameters.AddWithValue("@purchase_object_info", PurchaseObjectInfo) |> ignore
            cmd9.Parameters.AddWithValue("@type_fz", 5) |> ignore
            cmd9.Parameters.AddWithValue("@id_organizer", !IdOrg) |> ignore
            cmd9.Parameters.AddWithValue("@id_placing_way", !idPlacingWay) |> ignore
            cmd9.Parameters.AddWithValue("@id_etp", !idEtp) |> ignore
            cmd9.Parameters.AddWithValue("@end_date", endDate) |> ignore
            cmd9.Parameters.AddWithValue("@scoring_date", scoringDate) |> ignore
            cmd9.Parameters.AddWithValue("@bidding_date", biddingDate) |> ignore
            cmd9.Parameters.AddWithValue("@cancel", cancelStatus) |> ignore
            cmd9.Parameters.AddWithValue("@date_version", dateModified) |> ignore
            cmd9.Parameters.AddWithValue("@num_version", numVersion) |> ignore
            cmd9.Parameters.AddWithValue("@notice_version", NoticeVersion) |> ignore
            cmd9.Parameters.AddWithValue("@xml", tenurl) |> ignore
            cmd9.Parameters.AddWithValue("@print_form", Printform) |> ignore
            cmd9.ExecuteNonQuery() |> ignore
            idTender := int cmd9.LastInsertedId
            inc tenderCount
            let doctend = jsn.SelectToken("data.documents")
            if doctend <> null then 
                for doc in jsn.SelectToken("data.documents") do
                    let attachName = teststring <| doc.SelectToken("title")
                    let attachDescription = teststring <| doc.SelectToken("documentType")
                    let attachUrl = teststring <| doc.SelectToken("url")
                    if attachUrl <> "" then 
                        let insertAttach = 
                            sprintf 
                                "INSERT INTO %sattachment SET id_tender = @id_tender, file_name = @file_name, url = @url, description = @description" 
                                stn.Prefix
                        let cmd11 = new MySqlCommand(insertAttach, con)
                        cmd11.Prepare()
                        cmd11.Parameters.AddWithValue("@id_tender", !idTender) |> ignore
                        cmd11.Parameters.AddWithValue("@file_name", attachName) |> ignore
                        cmd11.Parameters.AddWithValue("@url", attachUrl) |> ignore
                        cmd11.Parameters.AddWithValue("@description", attachDescription) |> ignore
                        cmd11.ExecuteNonQuery() |> ignore
            printfn "%d" !idTender
        ()
    
    let parserL (o : JObject) (stn : Setting.T) (connectstring : string) : unit = 
        let uri = teststring <| o.SelectToken("next_page.uri")
        //printfn "%A" uri
        let tdata = o.SelectToken("data") :?> JArray
        for d in tdata do
            try 
                let id = d.SelectToken("id")
                using (new MySqlConnection(connectstring)) (ParserT d stn)
            with ex -> Logging.Log.logger(ex, id)
    
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
