open System.IO

module Helpers = 
    let split (separator: char) (str: string) = str.Split(separator) |> List.ofSeq
    let writeFile (path: string) (lines: string list) = File.WriteAllLines(path, lines)
    let readFile (path: string) = File.ReadAllText(path)

module Pipeline =
    type State<'a, 'b> = 
    | Ready of string
    | Read of 'a
    | Transformed of 'b
    | Written of bool

    type Steps<'a, 'b> = 
    | Reader of (string -> 'a)
    | Transformer of ('a -> 'b)
    | Writer of ('b -> bool)

    let rec apply (state: State<'a, 'b>) (step: Steps<'a, 'b>) = 
        match state, step with
        | Ready path, Reader next -> path |> next |> Read 
        | Read data, Transformer next -> data |> next |> Transformed 
        | Transformed data, Writer next -> data |> next |> Written
        | _, _ -> state

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

    let write (dest: string)(csv: CSV) = 
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

    let source = __SOURCE_DIRECTORY__ + "/data.csv"
    let dest = __SOURCE_DIRECTORY__ + "/data.txt"

    let pipeline = [
        read |> Reader
        transform |> Transformer
        write dest |> Writer
    ] 

    (Ready source, pipeline) ||> List.fold apply 
    
    
