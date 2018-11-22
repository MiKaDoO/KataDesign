open System.IO

module Helpers = 
    let split (separator: char) (str: string) = str.Split(separator) |> List.ofSeq
    let writeFile (path: string) (lines: string list) = File.WriteAllLines(path, lines)
    let readFile (path: string) = File.ReadAllText(path)

module Pipeline =
    type Read<'a> = Read of 'a
    type Transformed<'a> = Transformed of 'a
    type Written = bool

    type Path = string

    type Reader<'a> = Path -> 'a
    type Transformer<'a, 'b> = 'a -> 'b
    type Writer<'a> = 'a -> Path -> bool

    let private readWith (fn: Reader<'a>) = fn >> Read

    let private transformWith (fn: Transformer<'a, 'b>) = function 
        | Read d -> d |> fn |> Transformed
     
    let private writeWith (fn: Writer<'a>) = function
        | Transformed d -> d |> fn

    let buildPipeline (reader: Reader<'a>) (transformer: Transformer<'a, 'b>) (writer: Writer<'b>) = 
        readWith reader >> transformWith transformer >> writeWith writer

module PipelineCSV =
    open Helpers

    type CSV = {
        Headers: string list
        Rows: string list
    }

    let private getLines (content: string) = content |> split '\n'
    let private getColumns (line: string) = line |> split ';'

    let read (path: string) = path |> readFile |> getLines

    let transform (lines: string list) = 
        {
            Headers = lines.Head |> getColumns
            Rows = lines.Tail 
        }

    let write (csv: CSV) (dest: string) = 
        let formatRow (lineNumber: int) (line: string) = 
            line 
                |> getColumns
                |> List.zip csv.Headers
                |> List.fold (fun row (header, col) ->  sprintf "%s %s: %s " row header col) (sprintf "%i) " lineNumber)

        csv.Rows 
            |> List.mapi formatRow
            |> writeFile dest
        
        true

module Program =
    open Pipeline
    open PipelineCSV

    let pipeline = buildPipeline read transform write 
    
    (__SOURCE_DIRECTORY__ + "/data.csv", __SOURCE_DIRECTORY__ + "/data.txt") ||> pipeline
