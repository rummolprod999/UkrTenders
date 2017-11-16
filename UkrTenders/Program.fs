namespace Ukr
open System.Reflection
open System
open System.IO
module UkrModule =
    
    [<EntryPoint>]
    let main argv =  
        Parser.Parsing()
        0 // return an integer exit code

