namespace Ukr
open System.Reflection
open System
open System.IO
open System.Xml

module Setting =
    let GetPathProgram: string = 
            let path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().GetName().CodeBase)
            if path <> null then
                path.Substring(5)
            else ""
    type T = {Database : string; TempPathTenders: string; LogPathTenders : string; Prefix : string; UserDb : string; PassDb : string; Server : string; Port : int}
    
    let getSettings ():unit = 
        let xDoc = new XmlDocument()
        xDoc.Load()
        ()