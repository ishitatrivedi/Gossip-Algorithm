#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let system = ActorSystem.Create("GossipProtocol", configuration)
type Message = 
    | GossipMsg of (list<IActorRef>*IActorRef)
    | GossipMsgSelf of (list<IActorRef>*IActorRef)
    | PushSumMsg of (float*float*list<IActorRef>*IActorRef)
    | PushSumMsgSelf of (list<IActorRef>*IActorRef)
    | Init of (list<IActorRef>*string*IActorRef)
    | End of (IActorRef*IActorRef)

type BossMsg = 
    | Commence of (string)
    | MsgReceived of (string)

// Round off to proper cubes for 3D and imperfect 3D
let objStopwatch = System.Diagnostics.Stopwatch()

let createPerfectCubeNode (numNode:int) =
    let mutable cbrtVal=Math.Ceiling(System.Math.Pow(numNode |> float ,(1.0/3.0))) |>int
    cbrtVal*cbrtVal*cbrtVal

// Get input values from Command Line
let mutable intNodes = fsi.CommandLineArgs.[1] |> int
let strTopology = fsi.CommandLineArgs.[2]
let strAlgorithm = fsi.CommandLineArgs.[3]
let randNo = Random(intNodes)
if strTopology = "imp3D" || strTopology = "3D" then
    intNodes <- createPerfectCubeNode intNodes

// Create neighbours with Full Topology by setting all its neighbours except for itself
let createFullNeighbours (strActorName:string) (lstActor:list<IActorRef>) = 
    let idSelf = (strActorName.Split '_').[1] |> int
    let lstNeighbour = lstActor |> List.indexed |> List.filter (fun (i, _) -> i <> idSelf-1) |> List.map snd
    lstNeighbour
    
// Create neighbours with Line Topology by setting it's Left and Right neighbours
let createLineNeighbours (strActorName:string) (lstActor:list<IActorRef>) = 
    let mutable lstNeighbour = []
    let idSelf = (strActorName.Split '_').[1] |> int
    if idSelf = 1 then
       lstNeighbour <- lstActor.[idSelf] :: lstNeighbour 
    else if idSelf = intNodes then
       lstNeighbour <- lstActor.[idSelf-2] :: lstNeighbour 
    else
       lstNeighbour <- lstActor.[idSelf-2] :: lstNeighbour
       lstNeighbour <- lstActor.[idSelf] :: lstNeighbour
    lstNeighbour

// Creates neighbours according to the network topology of no of nodes = intNodes
let create3DNeighbours (strActorName:string) (lstActor:Message) = 
  match lstActor with
    | Init(poolActor, strTopo, boss) ->
        let idSelf = (strActorName.Split '_').[1] |> int
        let intSize=Math.Ceiling(System.Math.Pow(intNodes |> float ,(1.0/3.0))) |>int
        // let intSize=System.Math.Pow(intNodes |> float ,(0.33333)) |>int
        let mutable level=0
        if idSelf%(intSize*intSize) <> 0 then
            level<-(idSelf/(intSize*intSize))+1
        else
            level<-idSelf/(intSize*intSize)
        let mutable lstNeighbour = []
        if idSelf%intSize <> 1 then
            //make an edge to the right
            lstNeighbour <- poolActor.[idSelf-2] :: lstNeighbour
        if idSelf%intSize <> 0 then 
            //make an edge to the left
            lstNeighbour <- poolActor.[idSelf] :: lstNeighbour
        if level <> 1 then
            // make an edge down or below neighbour
            lstNeighbour <- poolActor.[idSelf-(intSize*intSize)-1] :: lstNeighbour
        if level <> intSize then
            // make an edge up or above neighbour
            lstNeighbour <- poolActor.[idSelf+(intSize*intSize)-1] :: lstNeighbour
        if idSelf > intSize + ((intSize*intSize)*(level-1)) then
            //make an edge at the front
            lstNeighbour <- poolActor.[idSelf-intSize-1] :: lstNeighbour
        if idSelf <= ((intSize*intSize)*level)-intSize then
            //make an edge at the back
            lstNeighbour <- poolActor.[idSelf+intSize-1] :: lstNeighbour
        if strTopo = "imp3D" then
            let mutable randomActor = poolActor.[randNo.Next()%intNodes]
            while randomActor.Path.Name = strActorName do
                randomActor <- poolActor.[randNo.Next()%intNodes]
            lstNeighbour <- randomActor :: lstNeighbour

        lstNeighbour

// Push-sum protocol implementation for aggregate calculation. State pair (s, w) is used to calculate the 
// convergence. The s/w ratio is added when recceived and halfed when sent to external neighbor from its
// list but remains same when message is sent the same actor itself
let PushSumProtoActor (mailbox:Actor<_>) =
    let mutable lstNeighbour = []
    let mutable lstAliveNeighbour = []
    let mutable neighbourCount = -1
    let mutable s = 0.0
    let mutable w = 1.0
    let mutable terminationCounter = 0
    let mutable oldVal = s/w
    let mutable strPushSumTopo = ""
    let mutable objBossReference = mailbox.Self
    let rec loop () = actor {
        let mutable init = false
        let! strPushMsg = mailbox.Receive()
        let mutable strPushMsg : Message = strPushMsg
        let mutable lstActor = []
        match strPushMsg with
        | PushSumMsgSelf(pool, bRef) ->
            lstActor <- pool
        | PushSumMsg(current_s,current_w,pool,boss) ->        
            s <- current_s + s
            w <- current_w + w
            lstActor <- pool
            if terminationCounter < 3 then  
                if abs ((s/w) - oldVal) <= (pown 10.0 -10) then
                    terminationCounter <- terminationCounter + 1
                    if terminationCounter = 3 then
                        objBossReference <! MsgReceived("Terminated")
                        lstNeighbour |> List.iter (fun item -> 
                            item <! End(mailbox.Self, objBossReference))
                else
                    terminationCounter <- 0 
        | Init(pool, strTopo, boss) ->
            init <- true
            objBossReference <- boss
            strPushSumTopo <- strTopo
            s <- (mailbox.Self.Path.Name.Split '_').[1] |> float
            oldVal <- s/w
            if strPushSumTopo = "3D" || strPushSumTopo = "imp3D" then
                lstNeighbour <- create3DNeighbours mailbox.Self.Path.Name strPushMsg
                lstAliveNeighbour <- create3DNeighbours mailbox.Self.Path.Name strPushMsg
            else if strPushSumTopo = "line" then 
                lstNeighbour <- createLineNeighbours mailbox.Self.Path.Name pool
                lstAliveNeighbour <- createLineNeighbours mailbox.Self.Path.Name pool
            else
                lstNeighbour <- createFullNeighbours mailbox.Self.Path.Name pool
                lstAliveNeighbour <- createFullNeighbours mailbox.Self.Path.Name pool
            neighbourCount <- lstNeighbour.Length
            return! loop()
        | End(killedActorRef, boss) ->
            let killedActorId = (killedActorRef.Path.Name.Split '_').[1] |> int
            neighbourCount <- neighbourCount - 1
            lstAliveNeighbour <- lstAliveNeighbour |> List.indexed |> List.filter (fun (i, v) -> ((v.Path.Name.Split '_').[1] |> int) <> killedActorId) |> List.map snd
            if neighbourCount = 0 then
                terminationCounter <- 100
                objBossReference <! MsgReceived("Terminated")
        | _ -> ignore()
    
        if terminationCounter <= 3 then   
            oldVal <- s/w
            s <- s/2.0
            w <- w/2.0
            if terminationCounter = 3 then
                terminationCounter <- 100
            let randPushSum = System.Random()
            if (strPushSumTopo = "full" && lstAliveNeighbour.Length <= 3) || (strPushSumTopo = "line") then
                let neighbour = lstAliveNeighbour.[randPushSum.Next(lstAliveNeighbour.Length)]
                neighbour <! PushSumMsg(s,w,lstActor,objBossReference)
            else
                let neighbour = lstNeighbour.[randPushSum.Next(lstNeighbour.Length)]
                neighbour <! PushSumMsg(s,w,lstActor,objBossReference)            
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, PushSumMsgSelf(lstActor, objBossReference))            

        return! loop()
    }
    loop()

// Gossip Actor Protocol implementation - receives rumors and sends rumours to one of it's neighbours depending on the topology and 
// also sends messages to itself periodically since it first receives the message until it terminates
let GossipProtoActors (mailbox:Actor<_>) = 
    let mutable isFirst = true
    let mutable exhaustedCount = 100
    let mutable lstNeighbour = []
    let mutable gossipTopo = ""
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let mutable gossipmsg : Message = message
        let mutable lstActor = []
        let mutable refBoss = mailbox.Self
        let mutable init = false
        match gossipmsg with
        | GossipMsg(pool, objBossReference) ->
            if isFirst then
                isFirst <- false
                objBossReference <! MsgReceived("Received")
            exhaustedCount <- exhaustedCount - 1
            lstActor <- pool
            refBoss <- objBossReference
        | GossipMsgSelf(pool,objBossReference) ->
            gossipmsg <- GossipMsg(pool,objBossReference)
            lstActor <- pool
            refBoss <- objBossReference
        | Init(pool, topo, boss) ->
            init <- true
            gossipTopo <- topo
            if topo = "3D" || topo = "imp3D" then
                lstNeighbour <- create3DNeighbours mailbox.Self.Path.Name gossipmsg
            else if topo = "line" then
                lstNeighbour <- createLineNeighbours mailbox.Self.Path.Name pool
            else 
                lstNeighbour <- createFullNeighbours mailbox.Self.Path.Name pool  
            return! loop()
        | _ -> ignore()

        if not init then
            if exhaustedCount >= 0 then
                let randImp3D = System.Random()
                if exhaustedCount = 0 then
                    exhaustedCount <- -1
                if lstNeighbour.Length > 0 then
                    let neighbour = lstNeighbour.[randImp3D.Next(lstNeighbour.Length)]
                    neighbour <! gossipmsg
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, GossipMsgSelf(lstActor, refBoss))
            else
                exhaustedCount <- -1
                mailbox.Context.Stop(mailbox.Self)

        return! loop()
    }
    loop()

// Initializes the list of actors and builds the network topology and randomly selects one actor to start with
// Terminates when all the actors have received the rumour 100 times for gossip
// Terminates in case of push-sum when s/w ratio is approximately same even after 3 rounds
let Boss (mailbox:Actor<_>) = 
    let mutable intNodesCovered = 0
    let rec loop () = actor {
        let! message = mailbox.Receive();
        match message with 
        | Commence(_) -> 
            if strAlgorithm = "gossip" then
                let lstActor = 
                    [1 .. intNodes]
                    |> List.map(fun id -> spawn system (sprintf "Actor_%d" id) GossipProtoActors)
                
                lstActor |> List.iter (fun item -> 
                    item <! Init(lstActor, strTopology, mailbox.Self))
                objStopwatch.Start()
                lstActor.[(randNo.Next()) % intNodes] <! GossipMsg(lstActor, mailbox.Self)                
            else if strAlgorithm = "push-sum" then
                let lstActor = 
                    [1 .. intNodes]
                    |> List.map(fun id -> spawn system (sprintf "Actor_%d" id) PushSumProtoActor)
                lstActor |> List.iter (fun item -> 
                    item <! Init(lstActor, strTopology, mailbox.Self))
                objStopwatch.Start()
                lstActor.[(randNo.Next()) % intNodes] <! PushSumMsgSelf(lstActor, mailbox.Self)
        | MsgReceived(_) ->
            intNodesCovered <- intNodesCovered + 1
            if intNodesCovered = intNodes then
                printfn "Time taken is %i\n" objStopwatch.ElapsedMilliseconds
                mailbox.Context.Stop(mailbox.Self)
                mailbox.Context.System.Terminate() |> ignore

        return! loop()
    }
    loop()

//Programs starts here where the boss is spawned
let boss = spawn system "boss" Boss
boss <! Commence("start")
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()