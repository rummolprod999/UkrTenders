namespace Ukr

module Parsing =
    let Parsing() = 
        let s = Download.DownloadString("https://stackoverflow.com/questions/15212133/increment-value-in-f")
        printf "%s" s