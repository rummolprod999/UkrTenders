namespace Ukr

open MySql.Data.MySqlClient
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System
open System.Collections.Generic
open System.Data
open System.Linq
open System.Text
open System.Text.RegularExpressions
open System.Xml

module Parsing =
    let tenderCount = ref 0
    let tenderUpCount = ref 0
    
    let testint (t : JToken) : int =
        match t with
        | null -> 0
        | _ -> (int) t
    
    let testfloat (t : JToken) : float =
        match t with
        | null -> 0.
        | _ -> (float) t
    
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
    
    let AddVerNumber (con : MySqlConnection) (pn : string) (stn : Setting.T) : unit =
        let verNum = ref 1
        let selectTenders =
            sprintf 
                "SELECT id_tender FROM %stender WHERE purchase_number = @purchaseNumber AND type_fz = 5 ORDER BY UNIX_TIMESTAMP(date_version) ASC" 
                stn.Prefix
        let cmd1 = new MySqlCommand(selectTenders, con)
        cmd1.Prepare()
        cmd1.Parameters.AddWithValue("@purchaseNumber", pn) |> ignore
        let dt1 = new DataTable()
        let adapter1 = new MySqlDataAdapter()
        adapter1.SelectCommand <- cmd1
        adapter1.Fill(dt1) |> ignore
        if dt1.Rows.Count > 0 then 
            let updateTender =
                sprintf "UPDATE %stender SET num_version = @num_version WHERE id_tender = @id_tender" stn.Prefix
            for ten in dt1.Rows do
                let idTender = (ten.["id_tender"] :?> int)
                let cmd2 = new MySqlCommand(updateTender, con)
                cmd2.Prepare()
                cmd2.Parameters.AddWithValue("@id_tender", idTender) |> ignore
                cmd2.Parameters.AddWithValue("@num_version", !verNum) |> ignore
                cmd2.ExecuteNonQuery() |> ignore
                incr verNum
        ()
    
    let TenderKwords (con : MySqlConnection) (idTender : int) (stn : Setting.T) : unit =
        let resString = new StringBuilder()
        let selectPurObj =
            sprintf 
                "SELECT DISTINCT po.name, po.okpd_name FROM %spurchase_object AS po LEFT JOIN %slot AS l ON l.id_lot = po.id_lot WHERE l.id_tender = @id_tender" 
                stn.Prefix stn.Prefix
        let cmd1 = new MySqlCommand(selectPurObj, con)
        cmd1.Prepare()
        cmd1.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt = new DataTable()
        let adapter = new MySqlDataAdapter()
        adapter.SelectCommand <- cmd1
        adapter.Fill(dt) |> ignore
        if dt.Rows.Count > 0 then 
            let distrDt = dt.AsEnumerable().Distinct(DataRowComparer.Default)
            for row in distrDt do
                let name =
                    match row.IsNull("name") with
                    | true -> ""
                    | false -> string <| row.["name"]
                
                let okpdName =
                    match row.IsNull("okpd_name") with
                    | true -> ""
                    | false -> string <| row.["okpd_name"]
                
                resString.Append(sprintf "%s %s " name okpdName) |> ignore
        let selectAttach = sprintf "SELECT file_name FROM %sattachment WHERE id_tender = @id_tender" stn.Prefix
        let cmd2 = new MySqlCommand(selectAttach, con)
        cmd2.Prepare()
        cmd2.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt2 = new DataTable()
        let adapter2 = new MySqlDataAdapter()
        adapter2.SelectCommand <- cmd2
        adapter2.Fill(dt2) |> ignore
        if dt2.Rows.Count > 0 then 
            let distrDt = dt2.AsEnumerable().Distinct(DataRowComparer.Default)
            for row in distrDt do
                let attName =
                    match row.IsNull("file_name") with
                    | true -> ""
                    | false -> string <| row.["file_name"]
                resString.Append(sprintf " %s" attName) |> ignore
        let idOrg = ref 0
        let selectPurInf =
            sprintf "SELECT purchase_object_info, id_organizer FROM %stender WHERE id_tender = @id_tender" stn.Prefix
        let cmd3 = new MySqlCommand(selectPurInf, con)
        cmd3.Prepare()
        cmd3.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt3 = new DataTable()
        let adapter3 = new MySqlDataAdapter()
        adapter3.SelectCommand <- cmd3
        adapter3.Fill(dt3) |> ignore
        if dt3.Rows.Count > 0 then 
            for row in dt3.Rows do
                let purOb =
                    match row.IsNull("purchase_object_info") with
                    | true -> ""
                    | false -> string <| row.["purchase_object_info"]
                idOrg := match row.IsNull("id_organizer") with
                         | true -> 0
                         | false -> row.["id_organizer"] :?> int
                resString.Append(sprintf " %s" purOb) |> ignore
        match (!idOrg) <> 0 with
        | true -> 
            let selectOrg =
                sprintf "SELECT full_name, inn FROM %sorganizer WHERE id_organizer = @id_organizer" stn.Prefix
            let cmd4 = new MySqlCommand(selectOrg, con)
            cmd4.Prepare()
            cmd4.Parameters.AddWithValue("@id_organizer", !idOrg) |> ignore
            let dt4 = new DataTable()
            let adapter4 = new MySqlDataAdapter()
            adapter4.SelectCommand <- cmd4
            adapter4.Fill(dt4) |> ignore
            if dt4.Rows.Count > 0 then 
                for row in dt4.Rows do
                    let innOrg =
                        match row.IsNull("inn") with
                        | true -> ""
                        | false -> string <| row.["inn"]
                    
                    let nameOrg =
                        match row.IsNull("full_name") with
                        | true -> ""
                        | false -> string <| row.["full_name"]
                    
                    resString.Append(sprintf " %s %s" innOrg nameOrg) |> ignore
        | false -> ()
        let selectCustomer =
            sprintf 
                "SELECT DISTINCT cus.inn, cus.full_name FROM %scustomer AS cus LEFT JOIN %spurchase_object AS po ON cus.id_customer = po.id_customer LEFT JOIN %slot AS l ON l.id_lot = po.id_lot WHERE l.id_tender = @id_tender" 
                stn.Prefix stn.Prefix stn.Prefix
        let cmd6 = new MySqlCommand(selectCustomer, con)
        cmd6.Prepare()
        cmd6.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt5 = new DataTable()
        let adapter5 = new MySqlDataAdapter()
        adapter5.SelectCommand <- cmd6
        adapter5.Fill(dt5) |> ignore
        if dt5.Rows.Count > 0 then 
            let distrDt = dt5.AsEnumerable().Distinct(DataRowComparer.Default)
            for row in distrDt do
                let innC =
                    match row.IsNull("inn") with
                    | true -> ""
                    | false -> string <| row.["inn"]
                
                let fullNameC =
                    match row.IsNull("full_name") with
                    | true -> ""
                    | false -> string <| row.["full_name"]
                
                resString.Append(sprintf " %s %s" innC fullNameC) |> ignore
        let mutable resS = Regex.Replace(resString.ToString(), @"\s+", " ")
        resS <- (resS.Trim())
        let updateTender =
            sprintf "UPDATE %stender SET tender_kwords = @tender_kwords WHERE id_tender = @id_tender" stn.Prefix
        let cmd5 = new MySqlCommand(updateTender, con)
        cmd5.Prepare()
        cmd5.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        cmd5.Parameters.AddWithValue("@tender_kwords", resS) |> ignore
        let res = cmd5.ExecuteNonQuery()
        if res <> 1 then Logging.Log.logger ("Не удалось обновить tender_kwords", idTender)
        ()
    
    let ParserT (d : JToken) (stn : Setting.T) (con : MySqlConnection) : unit =
        con.Open()
        let id = teststring <| d.SelectToken("id")
        let dateModified = testdate <| JsonConvert.SerializeObject(d.SelectToken("dateModified"))
        let dateModified =
            new DateTime(dateModified.Year, dateModified.Month, dateModified.Day, dateModified.Hour, dateModified.Minute, 
                         dateModified.Second, dateModified.Kind)
        let selectTend =
            sprintf 
                "SELECT id_tender FROM %stender WHERE id_xml = @id_xml AND date_version = @date_version AND type_fz = 5" 
                stn.Prefix
        let cmd : MySqlCommand = new MySqlCommand(selectTend, con)
        cmd.Prepare()
        cmd.Parameters.AddWithValue("@id_xml", id) |> ignore
        cmd.Parameters.AddWithValue("@date_version", dateModified) |> ignore
        let reader : MySqlDataReader = cmd.ExecuteReader()
        if reader.HasRows then 
            reader.Close()
            con.Close()
        else 
            reader.Close()
            let mutable cancelStatus = 0
            let mutable updated = false
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
                updated <- true
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
            let mutable startDate =
                testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.tenderPeriod.startDate"))
            if startDate = DateTime.MinValue then startDate <- dateModified
            //let startDate = enquiryPeriodstartDate
            let endDate = testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.tenderPeriod.endDate"))
            let biddingDate = testdate <| JsonConvert.SerializeObject(jsn.SelectToken("data.auctionPeriod.startDate"))
            let scoringDate = DateTime.MinValue
            let numVersion = 0
            let idTender = ref 0
            let insertTender =
                String.Format
                    ("INSERT INTO {0}tender SET id_xml = @id_xml, purchase_number = @purchase_number, doc_publish_date = @doc_publish_date, href = @href, purchase_object_info = @purchase_object_info, type_fz = @type_fz, id_organizer = @id_organizer, id_placing_way = @id_placing_way, id_etp = @id_etp, end_date = @end_date, scoring_date = @scoring_date, bidding_date = @bidding_date, cancel = @cancel, date_version = @date_version, num_version = @num_version, notice_version = @notice_version, xml = @xml, print_form = @print_form", 
                     stn.Prefix)
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
            match updated with
            | true -> incr tenderUpCount
            | false -> incr tenderCount
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
            let lotNumber = 1
            let idLot = ref 0
            let lotMaxPrice = testfloat <| jsn.SelectToken("data.value.amount")
            let lotCurrency = teststring <| jsn.SelectToken("data.value.currency")
            let insertLot =
                sprintf 
                    "INSERT INTO %slot SET id_tender = @id_tender, lot_number = @lot_number, max_price = @max_price, currency = @currency" 
                    stn.Prefix
            let cmd12 = new MySqlCommand(insertLot, con)
            cmd12.Parameters.AddWithValue("@id_tender", !idTender) |> ignore
            cmd12.Parameters.AddWithValue("@lot_number", lotNumber) |> ignore
            cmd12.Parameters.AddWithValue("@max_price", lotMaxPrice) |> ignore
            cmd12.Parameters.AddWithValue("@currency", lotCurrency) |> ignore
            cmd12.ExecuteNonQuery() |> ignore
            idLot := int cmd12.LastInsertedId
            let idCustomer = ref 0
            if identifierId <> "" then 
                let selectCustomer = sprintf "SELECT id_customer FROM %scustomer WHERE inn = @inn" stn.Prefix
                let cmd3 = new MySqlCommand(selectCustomer, con)
                cmd3.Prepare()
                cmd3.Parameters.AddWithValue("@inn", identifierId) |> ignore
                let reader = cmd3.ExecuteReader()
                match reader.HasRows with
                | true -> 
                    reader.Read() |> ignore
                    idCustomer := reader.GetInt32("id_customer")
                    reader.Close()
                | false -> 
                    reader.Close()
                    let insertCustomer =
                        sprintf "INSERT INTO %scustomer SET reg_num = @reg_num, full_name = @full_name, inn = @inn" 
                            stn.Prefix
                    let RegNum = Guid.NewGuid().ToString()
                    let legalName = teststring <| jsn.SelectToken("data.procuringEntity.identifier.legalName")
                    let cmd14 = new MySqlCommand(insertCustomer, con)
                    cmd14.Prepare()
                    cmd14.Parameters.AddWithValue("@reg_num", RegNum) |> ignore
                    cmd14.Parameters.AddWithValue("@full_name", legalName) |> ignore
                    cmd14.Parameters.AddWithValue("@inn", identifierId) |> ignore
                    cmd14.ExecuteNonQuery() |> ignore
                    idCustomer := int cmd14.LastInsertedId
            let items = jsn.SelectToken("data.items")
            if items <> null then 
                let GuaranteeAmount = testfloat <| jsn.SelectToken("data.guarantee.amount")
                for it in items do
                    let description = teststring <| it.SelectToken("description")
                    let quantity = testfloat <| it.SelectToken("quantity")
                    let UnitName = teststring <| it.SelectToken("unit.name")
                    let scheme = teststring <| it.SelectToken("classification.id")
                    let classdescript = teststring <| it.SelectToken("classification.description")
                    let insertLotitem =
                        sprintf 
                            "INSERT INTO %spurchase_object SET id_lot = @id_lot, id_customer = @id_customer, okpd2_code = @okpd2_code, okpd_name = @okpd_name, name = @name, quantity_value = @quantity_value, okei = @okei, customer_quantity_value = @customer_quantity_value" 
                            stn.Prefix
                    let cmd19 = new MySqlCommand(insertLotitem, con)
                    cmd19.Prepare()
                    cmd19.Parameters.AddWithValue("@id_lot", !idLot) |> ignore
                    cmd19.Parameters.AddWithValue("@id_customer", !idCustomer) |> ignore
                    cmd19.Parameters.AddWithValue("@okpd2_code", scheme) |> ignore
                    cmd19.Parameters.AddWithValue("@okpd_name", classdescript) |> ignore
                    cmd19.Parameters.AddWithValue("@name", description) |> ignore
                    cmd19.Parameters.AddWithValue("@quantity_value", quantity) |> ignore
                    cmd19.Parameters.AddWithValue("@okei", UnitName) |> ignore
                    cmd19.Parameters.AddWithValue("@customer_quantity_value", quantity) |> ignore
                    cmd19.ExecuteNonQuery() |> ignore
                    let postalCode = teststring <| it.SelectToken("deliveryAddress.postalCode")
                    let countryName = teststring <| it.SelectToken("deliveryAddress.countryName")
                    let streetAddress = teststring <| it.SelectToken("deliveryAddress.streetAddress")
                    let region = teststring <| it.SelectToken("deliveryAddress.region")
                    let locality = teststring <| it.SelectToken("deliveryAddress.locality")
                    let deliveryPlace =
                        (sprintf "%s %s %s %s %s" postalCode countryName region locality streetAddress).Trim()
                    let startDate = teststring <| it.SelectToken("deliveryDate.startDate")
                    let endDate = teststring <| it.SelectToken("deliveryDate.endDate")
                    let deliveryTerm = sprintf "Start date: %s \n End date: %s" startDate endDate
                    let insertCustomerRequirement =
                        sprintf 
                            "INSERT INTO %scustomer_requirement SET id_lot = @id_lot, id_customer = @id_customer, delivery_place = @delivery_place, delivery_term = @delivery_term, application_guarantee_amount = @application_guarantee_amount" 
                            stn.Prefix
                    let cmd16 = new MySqlCommand(insertCustomerRequirement, con)
                    cmd16.Prepare()
                    cmd16.Parameters.AddWithValue("@id_lot", !idLot) |> ignore
                    cmd16.Parameters.AddWithValue("@id_customer", !idCustomer) |> ignore
                    cmd16.Parameters.AddWithValue("@delivery_place", deliveryPlace) |> ignore
                    cmd16.Parameters.AddWithValue("@delivery_term", deliveryTerm) |> ignore
                    cmd16.Parameters.AddWithValue("@application_guarantee_amount", GuaranteeAmount) |> ignore
                    cmd16.ExecuteNonQuery() |> ignore
            try 
                AddVerNumber con tenderID stn
            with ex -> 
                Logging.Log.logger "Ошибка добавления версий тендера"
                Logging.Log.logger ex
            try 
                TenderKwords con (!idTender) stn
            with ex -> 
                Logging.Log.logger "Ошибка добавления kwords тендера"
                Logging.Log.logger ex
        ()
    
    //printfn "%d" !idTender
    let parserL (o : JObject) (stn : Setting.T) (connectstring : string) : unit =
        //let uri = teststring <| o.SelectToken("next_page.uri")
        //printfn "%A" uri
        let tdata = o.SelectToken("data") :?> JArray
        for d in tdata do
            let id = d.SelectToken("id")
            try 
                using (new MySqlConnection(connectstring)) (ParserT d stn)
            with ex -> Logging.Log.logger (ex, id)
    
    let ParsungListTenders (j : JObject) (sett : Setting.T) =
        let connectstring =
            sprintf 
                "Server=%s;port=%d;Database=%s;User Id=%s;password=%s;CharSet=utf8;Convert Zero Datetime=True;default command timeout=3600;Connection Timeout=3600;SslMode=none" 
                sett.Server sett.Port sett.Database sett.UserDb sett.PassDb
        try 
            parserL j sett connectstring
        with x -> Logging.Log.logger x
    
    let Parsing(st : Setting.T) =
        //let s = Download.DownloadString("https://stackoverflow.com/questions/15212133/increment-value-in-f")
        let startUrl =
            sprintf "http://public.api.openprocurement.org/api/2.4/tenders?offset=%s" 
            <| DateTime.Now.AddHours(-100.).ToString("s")
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
                | (x, _) -> 
                    ParsungListTenders json st
                    urlDown <- x
