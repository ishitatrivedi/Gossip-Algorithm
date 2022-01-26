#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

// Configuration
let objConfig = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let system = ActorSystem.Create("GossipProtocol", objConfig)
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
        let selfId = (strActorName.Split '_').[1] |> int
        let intSize=System.Math.Pow(intNodes |> float ,(0.34)) |>int
        let mutable level = 0
        if selfId%(intSize*intSize) <> 0 then
            level<-(selfId/(intSize*intSize))+1
        else
            level<-selfId/(intSize*intSize)
        let mutable lstNeighbour = []
        if selfId%intSize <> 1 then
            //make an edge to the right
            lstNeighbour <- poolActor.[selfId-2] :: lstNeighbour
        if selfId%intSize <> 0 then 
            //make an edge to the left
            lstNeighbour <- poolActor.[selfId] :: lstNeighbour
        if level <> 1 then
            // make an edge down or below neighbour
            lstNeighbour <- poolActor.[selfId-(intSize*intSize)-1] :: lstNeighbour
        if level <> intSize then
            // make an edge up or above neighbour
            lstNeighbour <- poolActor.[selfId+(intSize*intSize)-1] :: lstNeighbour
        if selfId > intSize + ((intSize*intSize)*(level-1)) then
            //make an edge at the front
            lstNeighbour <- poolActor.[selfId-intSize-1] :: lstNeighbour
        if selfId <= ((intSize*intSize)*level)-intSize then
            //make an edge at the back
            lstNeighbour <- poolActor.[selfId+intSize-1] :: lstNeighbour
        if strTopo = "imp3D" then
            let mutable randomActor = poolActor.[randNo.Next()%intNodes]
            while randomActor.Path.Name = strActorName do
                randomActor <- poolActor.[randNo.Next()%intNodes]
            lstNeighbour <- randomActor :: lstNeighbour

        // printfn "selfId - %d" selfId
        // printfn "lstNeighbour - %A" lstNeighbour
        lstNeighbour

// Push-sum protocol implementation for aggregate calculation. State pair (s, w) is used to calculate the 
// convergence. The s/w ratio is added when recceived and halfed when sent to external neighbor from its
// list but remains same when message is sent the same actor itself
let PushSumProtoActor (mailbox:Actor<_>) =
    let mutable lstNeighbour = []
    let mutable s = 0.0
    let mutable w = 1.0
    let mutable terminationCounter = 0
    let mutable oldVal = s/w
    let mutable strPushSumTopo = ""
    let mutable objBossReference = mailbox.Self
    let rec loop () = actor {
        let! strMessage = mailbox.Receive()
        let mutable strPushMsg : Message = strMessage
        let mutable lstActor = []
        match strPushMsg with
        | PushSumMsgSelf(pool, bRef) ->
            lstActor <- pool
        | PushSumMsg(current_s,current_w,pool,boss) ->
            lstActor <- pool
            if terminationCounter < 3 then
                s <- current_s + s
                w <-current_w + w
                if abs ((s/w) - oldVal) <= (pown 10.0 -10) then
                    terminationCounter <- terminationCounter + 1
                    if terminationCounter = 3 then
                        objBossReference <! MsgReceived("Terminated")
                        lstNeighbour |> List.iter (fun item -> 
                            item <! End(mailbox.Self, objBossReference))
                else
                    terminationCounter <- 0
        | Init(pool, strTopo, boss) ->
            strPushSumTopo <- strTopo
            objBossReference <- boss
            s <- (mailbox.Self.Path.Name.Split '_').[1] |> float
            oldVal <- s/w
            if strPushSumTopo = "3D" || strPushSumTopo = "imp3D" then
                lstNeighbour <- create3DNeighbours mailbox.Self.Path.Name strPushMsg
            else if strPushSumTopo = "line" then 
                lstNeighbour <- createLineNeighbours mailbox.Self.Path.Name pool
            else
                lstNeighbour <- createFullNeighbours mailbox.Self.Path.Name pool
            return! loop()
        | End(killedActorRef, boss) ->
            let killedActorId = (killedActorRef.Path.Name.Split '_').[1] |> int
            lstNeighbour <- lstNeighbour |> List.indexed |> List.filter (fun (i, v) -> ((v.Path.Name.Split '_').[1] |> int) <> killedActorId) |> List.map snd

            if lstNeighbour.Length = 0 then
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
            if lstNeighbour.Length > 0 then
                let neighbour = lstNeighbour.[randPushSum.Next(lstNeighbour.Length)]
                neighbour <! PushSumMsg(s,w,lstActor,objBossReference)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, PushSumMsgSelf(lstActor, objBossReference))

        return! loop()
    }
    loop()

// Gossip Actor Protocol implementation - receives rumors and sends rumours to one of it's neighbours depending on the topology and 
// also sends messages to itself periodically since it first receives the message until it terminates
let GossipProtoActors (mailbox:Actor<_>) = 
    let mutable exhaustedCount = 100
    let mutable lstNeighbour = []
    let mutable neighbourCount = -1
    let mutable gossipTopo = ""
    let mutable refBoss = mailbox.Self
    let mutable isGossipSpread = false
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let mutable gossipmsg : Message = message
        let mutable lstActor = []
        let mutable init = false
        match gossipmsg with
        | GossipMsg(pool, objBossReference) ->
            exhaustedCount <- exhaustedCount - 1
            lstActor <- pool
        | GossipMsgSelf(pool,objBossReference) ->
            gossipmsg <- GossipMsg(pool,objBossReference)
            lstActor <- pool
        | Init(pool, topo, boss) ->
            init <- true
            gossipTopo <- topo
            if topo = "3D" || topo = "imp3D" then
                lstNeighbour <- create3DNeighbours mailbox.Self.Path.Name gossipmsg
            else if topo = "line" then
                lstNeighbour <- createLineNeighbours mailbox.Self.Path.Name pool
            else 
                lstNeighbour <- createFullNeighbours mailbox.Self.Path.Name pool  
            neighbourCount <- lstNeighbour.Length
            refBoss <- boss
            return! loop()
        | End(killedActorRef, objBossReference) ->
            neighbourCount <- neighbourCount - 1
            if neighbourCount = 0 then
                isGossipSpread <- true
                exhaustedCount <- -1
                objBossReference <! MsgReceived("Terminated")
        | _ -> ignore()

        if exhaustedCount >= 0 then
            let randGossip = System.Random()
            if lstNeighbour.Length > 0 then
                let neighbour = lstNeighbour.[randGossip.Next(lstNeighbour.Length)]
                neighbour <! gossipmsg
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, GossipMsgSelf(lstActor, refBoss))
        else if not isGossipSpread then
            isGossipSpread <- true
            exhaustedCount <- -1
            refBoss <! MsgReceived("Terminated")
            lstNeighbour |> List.iter (fun item -> 
                item <! End(mailbox.Self, refBoss))
        else
            exhaustedCount <- -1

        return! loop()
    }
    loop()

// Initializes the list of actors and builds the network topology and randomly selects one actor to start with
// Terminates when all the actors have received the rumour 100 times for gossip
// Terminates in case of push-sum when s/w ratio is approximately same even after 3 rounds
let Boss (mailbox:Actor<_>) = 
    let mutable intNodesCovered = 0
    let mutable count = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
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
        | MsgReceived(name) -> 
            if name = "First" then
                count <- count + 1  
            else 
                intNodesCovered <- intNodesCovered + 1
                if intNodesCovered = intNodes then
                    printfn "Time taken is %i\n" objStopwatch.ElapsedMilliseconds
                    mailbox.Context.Stop(mailbox.Self)
                    mailbox.Context.System.Terminate() |> ignore
        | _ -> ()

        return! loop()
    }
    loop()

//Programs starts here where the boss is spawned
let boss = spawn system "boss" Boss
boss <! Commence("start")
system.WhenTerminated.Wait()