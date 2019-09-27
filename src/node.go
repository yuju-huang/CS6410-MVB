package main

import (
   "fmt"
   "flag"
   "strings"
   "strconv"
//   "bytes"
   "os"
   "net"
//   "encoding/hex"
   "crypto/sha256"
   "sync"
   "time"
//   "unsafe"
   "bufio"
   "math/rand"
//   "runtime/pprof"
)

var transactionSeen map[string]bool
var tseenmutex sync.Mutex
var blockSeen map[[32]byte]bool
var bseenmutex sync.Mutex
var accountAmount map[string]int
var aamountmutex sync.Mutex
var blockmsgs [][]byte

var timeChan = make(chan int) // should not be global

//for debugging
var debugFlag = false

//for profiling
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
   var port string
   var peersStr string
   var numtxinblock int
   var difficulty int
   var numcores int

/*
   //don't know how to use, give up anyway
   if *cpuprofile != "" {
      cpuProfileFile, err := os.Create(*cpuprofile)
      if err != nil {
        print("could not create CPU profile: ", err)
    }
    defer cpuProfileFile.Close()
    if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
        print("could not start CPU profile: ", err)
    }
    defer pprof.StopCPUProfile()
   }
*/

   flag.StringVar(&port, "port", "", "The port for this node to listen on")
   flag.StringVar(&peersStr, "peers", "", "The list of peers for this node to broadcast to")
   flag.IntVar(&numtxinblock, "numtxinblock", 50000, "The number of transactions in each block")
   flag.IntVar(&difficulty, "difficulty", 1, "The number of leading bytes that must be zero during mining")
   flag.IntVar(&numcores, "numcores", 1, "The number of independent processor cores to be used for mining")
   flag.Parse()

   //ensure that a port flag was given
   if(port == "") {
      flag.Usage()
      os.Exit(1)
   }

   //setting random seed
   rand.Seed(time.Now().UTC().UnixNano())

   //create a list of peer ports to connect to
   peers := strings.Split(peersStr, ",")

   fmt.Println("port:", port)
   fmt.Println("peers:", peers)

   //Set up data structures

   //map to track which transactions have been seen in order to prevent duplicate transactions
   //NOTE: in go, the default boolean value is "False", so this will return False for unseen entries.
   transactionSeen = make(map[string]bool)

   //map to track which blocks have been seen in order to prevent processing duplicate blocks
   blockSeen = make(map[[32]byte]bool)

   //list of all "block found" messages generated at this node (used to respond to get_block)
   blockmsgs = make([][]byte, 0)

   //initialize the account amounts, which is used to catch double spending.
   accountAmount = make(map[string]int)
   for i:=0; i<100; i++ {
      account := sha256.Sum256([]byte(strconv.Itoa(i)))
      accountAmount[string(account[:])] = 100000
   }

   //Anything sent to this channel will be broadcast to all peers.
   broadcast := make(chan []byte, 5)

   //When shutting down the node, broadcastgroup will allow us to wait for the
   //broadcast goroutine to close cleanly before the main goroutine exits.
   //This ensures that the shut down signal is properly forwarded to all peers
   //before exiting.
   var broadcastgroup sync.WaitGroup
   broadcastgroup.Add(1)

   //This goroutine broadcasts anything received on the broadcast channel to each of the peers.
   go broadcaster(broadcast, peers, &broadcastgroup)

   //Print average time in handling each block.
   go countAverageMiningTime()

   //txchan is used to communicate incoming transactions to mineManager so that
   //they can be included in future mined blocks.
   txchan := make(chan []byte)
   var mmgroup sync.WaitGroup
   mmgroup.Add(1)
   //Start the mine manager...
   go mineManager(txchan, broadcast, numtxinblock, difficulty, numcores, &mmgroup)

   //Set up shutdown channel and wait group
   shutdown := make(chan bool)
   var shutonce sync.Once
   var conngroup sync.WaitGroup

   //Start listening on the correct port
   addr, err := net.ResolveTCPAddr("tcp", "localhost:"+port)
   l, err := net.ListenTCP("tcp", addr)
   if(err != nil) {
      fmt.Println("Error listening: ", err.Error())
      os.Exit(1)
   }
   defer l.Close()
   fmt.Println("Listening on localhost:"+port)

   //Now, we want to accept new connections, while also being ready to cleanly shut down
   //the program in the event that we get a shutdown message.
   for {
      select {
      case <- shutdown:
         println("shutdown")
         conngroup.Wait()
         println("All connection goroutines closed")
         close(txchan)
         println("Waiting for mining to complete")
         mmgroup.Wait()
         println("Mining complete.")
         close(broadcast)
         println("Waiting for peer broadcaster to close")
         broadcastgroup.Wait()
         println("Peer broadcaster closed.")
         return
      default:
         l.SetDeadline(time.Now().Add(100*time.Millisecond))
         conn, err := l.AcceptTCP()
         if(err == nil) {
            conngroup.Add(1)
            go handleIncomingConnection(conn, broadcast, txchan, shutdown, &shutonce, &conngroup, numtxinblock)
         }
      }
   }
}

func countAverageMiningTime() {
   numBlock := 0
   totalTime := 0
   for {
      select {
         case time := <-timeChan:
            numBlock += 1
            totalTime += time
            fmt.Println("Average mining time:", (totalTime/numBlock)/1000,
                        " us for mining ", numBlock, " blocks in total")
         default:
            time.Sleep(50 * time.Millisecond)
      }
   }
}

//This goroutine connects to the peers indicated in the command line arguments.
//In this assignment, all messages that are sent to peers must be broadcast to all peers
func broadcaster(broadcast chan []byte, peers []string, broadcastgroup *sync.WaitGroup){
   defer broadcastgroup.Done()

   //Defer connecting to peers until first message needs to be sent. Otherwise, other
   //nodes may not yet be started when we try to connect to them.
   msg, more := <-broadcast

   fmt.Println("Connecting to peers...")

   peerConnections := make([]net.Conn, len(peers))
   for i, peer := range peers {
      //For each peer, open a socket to that peer using net.Dial
      fmt.Println("Starting connection to localhost:", peer)
      peerConnections[i], _ = net.Dial("tcp", "localhost:"+peer)
      //When the broadcaster goroutine exits, cleanly close all peer connections.
      defer peerConnections[i].Close()
   }

   for {
      if !more {
         fmt.Println("Sending final close signal to all peers");
         for _, conn := range peerConnections {
            conn.Write([]byte{'1'})
         }
         return
      }
      for _, conn := range peerConnections {
         if debugFlag {
            // fmt.Println("forwarding message to ", conn.RemoteAddr())
         }
         conn.Write(msg)
      }
      msg, more = <-broadcast
   }
}


func handleIncomingConnection(conn *net.TCPConn, broadcast chan []byte, txchan chan []byte, shutdown chan bool, shutonce *sync.Once, conngroup *sync.WaitGroup, numtxinblock int) {
   fmt.Println("Got connection from ", conn.RemoteAddr())
   //Close the TCP connection cleanly when the goroutine exits
   defer conn.Close()
   //Indicate to the waitgroup when this goroutine exits
   defer conngroup.Done()

   reader := bufio.NewReader(conn)

   for {
      select {
      case <- shutdown:
         return
      default:
         conn.SetDeadline(time.Now().Add(100*time.Millisecond))
         opcode, err := reader.ReadByte()
         if err == nil {
            switch opcode {
            case '0':
               //fmt.Println("Transaction message received")

               //A transaction message is 128 bytes long. However, in this case
               //we want to include the opcode, since we will be forwarding this
               //message to peers.
               readTotal := 1
               buf := make([]byte, 129)

               //The first byte of buf should be the opcode.
               buf[0] = '0'

               for readTotal < 129 {
                  readNow, _ := reader.Read(buf[readTotal:])
                  readTotal += readNow
               }
               handleTransaction(buf, broadcast, txchan, conn)

            case '1':
               fmt.Println("Close message received")

               //Closing the shutdown channel functions effectively like a broadcast,
               //since every goroutine listening to the shutdown channel will be alerted.
               //using a sync.Once ensures that this step is performed only once even if
               //two handleIncomingConnection goroutines receive a close signal at the
               //same time. (closing an already closed channel would cause an exception)
               shutonce.Do(func() {close(shutdown)})

               return

            case '2':
               fmt.Println("Block message recived")

               //A block message is 160 + 128*numtxinblock bytes long.
               //However, in this case we want to include the opcode, since we will
               //be forwarding this block message to peers.

               readTotal := 1
               buf := make([]byte, 1 + 160 + 128*numtxinblock)

               //The first byte of buf should be the opcode.
               buf[0] = '2'

               for readTotal < (1 + 160 + 128*numtxinblock) {
                  readNow, _ := reader.Read(buf[readTotal:])
                  readTotal += readNow
               }
               var blockhash [32]byte
               copy(blockhash[:], buf[65:97])
               bseenmutex.Lock()
               seen := blockSeen[blockhash]
               blockSeen[blockhash] = true
               bseenmutex.Unlock()
               if !seen {
                  fmt.Println("First time seeing this block: broadcasting...")
                  broadcast <- buf
               }

            case '3':
               println("get_block message received")

               //A get_block message is 32 bytes long.
               //This time, we will not be forwarding the message, so there
               //is no need to include the opcode.

               readTotal := 0
               buf := make([]byte, 32)
               for readTotal < 32 {
                  readNow, _ := reader.Read(buf[readTotal:])
                  readTotal += readNow
               }

               height, err := strconv.Atoi(string(buf[0:32]))
               if(err != nil) {
                  fmt.Println("Error interpreting amount:", err.Error())
                  continue
               }
               if height < len(blockmsgs) {
                  println("sending block at height ", height)
                  conn.Write(blockmsgs[height])
               }
            default:
               fmt.Println("Unknown message received")
            }
         }
      }
   }
}

func handleTransaction(buf []byte, broadcast chan []byte, txchan chan []byte, conn *net.TCPConn) {
   //do not process transactions that have already been seen
   transactionStr := string(buf)
   tseenmutex.Lock()
   if transactionSeen[transactionStr] {
      if debugFlag {
         fmt.Println("[Warning] Transaction seen: ", buf)
      }
      tseenmutex.Unlock()
      return
   } else {
      transactionSeen[transactionStr] = true
   }
   tseenmutex.Unlock()

   sender := buf[1:33]
   receiver := buf[33:65]
   amount, err := strconv.Atoi(string(buf[65:97]))
   if(err != nil) {
      fmt.Println("Error interpreting amount:", err.Error())
      return
   }

   //check that the transaction has valid sender and receiver, and that the sender has enough funds.
   senderStr := string(sender)
   receiverStr := string(receiver)

   validTransaction := false
   aamountmutex.Lock()
   _, receiverValid := accountAmount[receiverStr]
   balance, senderValid := accountAmount[senderStr]

   if receiverValid && senderValid {
      if (balance >= amount) {
         validTransaction = true
         accountAmount[senderStr] -= amount
         accountAmount[receiverStr] += amount
      } else {
         if debugFlag {
            fmt.Println("[Warning] Double spending")
         }
      }
   }
   aamountmutex.Unlock()

   if validTransaction {
      tx := append([]byte(nil), buf[0:129]...)
      broadcast <- tx
      txchan <- tx[1:129]
   }
}


func mineManager(txchan chan []byte, broadcast chan []byte, numtxinblock int, difficulty int, numcores int, mmgroup *sync.WaitGroup) {
   defer mmgroup.Done()

   blockdata := make([][]byte, 0)
   blockdata = append(blockdata, make([]byte, 0, numtxinblock*128))
   currentnumtx := 0
   currenthead := 0
   currentlymining := false
   var mineinfo []byte

   currentheight := 0
   currenthash := sha256.Sum256([]byte{0})
   mineraddr := sha256.Sum256([]byte("yh885"))

   noncechan := make(chan [32]byte)

   startMining := func() {
      currentlymining = true
      mineinfo = make([]byte, 0, 128*(numtxinblock+1))
      mineinfo = append(mineinfo, make([]byte, 32)...)
      mineinfo = append(mineinfo, currenthash[:]...)
      mineinfo = append(mineinfo, []byte(fmt.Sprintf("%032d",currentheight))...)
      mineinfo = append(mineinfo, mineraddr[:]...)
      mineinfo = append(mineinfo, blockdata[currentheight]...)
      if debugFlag {
         println("starting to mine block number ", currentheight)
      }
      go mine(difficulty, mineinfo, noncechan, numcores)
   }

   //The main loop of the mine manager. While txchan is open, we want to try to receive
   //new transactions and record them so that they can be mined. In addition, if our mining
   //goroutine finds a block, we want to update our current height (since we found a block)
   //and begin mining the next block if there is one available.
   mainloop:
   for {
      select {
      case tx, more := <-txchan:
         if !more {
            //The main goroutine has closed txchan, indicating that we are done receiving
            //new transactions. However, we still have to finish mining blocks for any
            //already received transactions.
            break mainloop
         }
         blockdata[currenthead] = append(blockdata[currenthead], tx...)
         currentnumtx += 1
         if currentnumtx == numtxinblock {
            blockdata = append(blockdata, make([]byte, 0, numtxinblock*128))
            currentnumtx = 0
            currenthead++
            if !currentlymining {
               startMining()
            }
         }
      case nonce := <-noncechan:
         if debugFlag {
            println("got a nonce")
         }
         currentlymining = false

         //compute the hash based on the nonce
         minedBlock := append(nonce[:], mineinfo[32:]...)
         hash := sha256.Sum256(minedBlock)

         //add the hash to our list of processed blocks.
         bseenmutex.Lock()
         blockSeen[hash] = true
         bseenmutex.Unlock()

         //generate the Block message to advertize this block to other nodes.
         //minedBlock: [nonce|prior_hash, hash, block_height, miner_address, txs]
         msg := append([]byte("2"), minedBlock[0:64]...)
         msg = append(msg, hash[:]...)
         msg = append(msg, minedBlock[64:]...)

         //add this block message to blockmsgs.
         //this allows us to respond to future get_block requests.
         blockmsgs = append(blockmsgs, msg)

         //broadcast the block message.
         broadcast <- msg

         //update currentheight and check if there is another block ready to mine.
         currentheight ++
         if(currenthead > currentheight){
            startMining()
         }
      }
   }

   //If we get a shutdown message, the main goroutine will close txchan. However,
   //according to the spec, node should finish mining all blocks that it currently
   //has enough transactions to fill before shutting down. Therefore, we mine those
   //blocks before we terminate the mine manager.
   for currenthead > currentheight {
      if !currentlymining {
         startMining()
      }
      nonce := <-noncechan
      println("got a block")
      currentlymining = false

      //compute the hash based on the nonce
      minedBlock := append(nonce[:], mineinfo[32:]...)
      hash := sha256.Sum256(minedBlock)

      //add the hash to our list of processed blocks.
      bseenmutex.Lock()
      blockSeen[hash] = true
      bseenmutex.Unlock()

      //generate the Block message to advertize this block to other nodes.
      msg := append([]byte("2"), minedBlock[0:64]...)
      msg = append(msg, hash[:]...)
      msg = append(msg, minedBlock[64:]...)

      //broadcast the block message.
      broadcast <- msg

      //add this block message to blockmsgs.
      //this allows us to respond to future get_block requests.
      blockmsgs = append(blockmsgs, msg)

      //update currentheight and check if there is another block ready to mine.
      currentheight ++
   }

}

func mine(difficulty int, block []byte, noncechan chan [32]byte, numcores int) {
   var nonce [32]byte

   tmpNonceChan := make(chan []byte)

   validateBlock := func(hash [32]byte) bool {
      for i := 0; i < difficulty; i++ {
         if hash[i] != 0 {
            return false
         }
      }
      return true
   }

   var done sync.WaitGroup
   done.Add(numcores)
   mined := false
   var minedMutex sync.Mutex

   duplicateBlocks := make([][]byte, numcores)

   t1 := time.Now()
   //1. create goroutine according to numcores
   for i:=0; i<numcores; i++ {
      //duplicate block
      duplicateBlock := append(duplicateBlocks[i], block...)

      //mineImpl for finding nonce in parallel
      go func(block []byte, noncechan chan []byte) {
         tmpNonce := make([]byte, 32)

         for {
            if mined {
               done.Done()
               return
            }

            //generate random nonce
            rand.Read(tmpNonce)

            //find a nonce that results in a hash with enough leading zeroes.
            copy(block[0:32], tmpNonce)
            hash := sha256.Sum256(block)
            if validateBlock(hash) {
               minedMutex.Lock()
               if mined {
                  done.Done()
                  minedMutex.Unlock()
                  return
               }

               mined = true
               noncechan <- tmpNonce
               if debugFlag {
                  fmt.Println("hash valid!")
                  fmt.Println("hash:", hash)
               }
               minedMutex.Unlock()
           }
        }
      }(duplicateBlock, tmpNonceChan)
   }

   //2. if nonce is updated, use it and return
   tmpNonce := <-tmpNonceChan
   t2 := time.Now()
   done.Wait()
   close(tmpNonceChan)
   mined = false

   timeDiff := t2.Sub(t1)
   if debugFlag {
      fmt.Println("Use ", timeDiff, " to mine the block")
   }
   timeChan <- int(timeDiff)

   copy(nonce[:], tmpNonce)
   noncechan <- nonce
}
