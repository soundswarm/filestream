const testFolder = './logFiles';
const fsPromise = require('fs-promise');
const fs = require ('fs');
const Heap = require('heap');

// read file names from directory
const readFiles = (files)=>{
  return fsPromise.readdir(files)
}

// read line of log file starting at the byte position indicated by startLine
const readLine = (fileDir, startLine, key)=>{

  // promisify streaming data
  return new Promise((resolve, reject)=>{ 
    const options = {
      encoding: 'utf8',
      start: startLine //byte position of line to read
    }

    const stream = fs.createReadStream(fileDir, options)
    
    stream.on('end', ()=>{
      return resolve({
        end: true,
        key
      })
    })

    let data = ''
    stream.on("data", function (moreData) {
      data += moreData;
      lines = data.split("\n");
      if (lines.length > 1) {
        stream.destroy();
        let log = lines.slice(0, 1)[0]; 
        const logSize = Buffer.byteLength(log, 'utf8')
        log = JSON.parse(log);
        const newStartLine = startLine+logSize+1; // add 1 to account for new line character
        return resolve({
          log,
          newStartLine,
          key
        })
      }
    })
  })
}

const startProcess = ()=>{

  // a heap to store the logs that have been ready from files before theyre printed to the console
  const heap = new Heap((a,b)=>{
    return a.log.time - b.log.time
  })

  const startTime = Date.now() // used to determin run time of process
  readFiles(testFolder)
  .then(files=>{
    let stateMap = {}; //this holds the minimum time from each file that has not been printed to console
    // initialize stateMap
    files.forEach( (file,i)=>{
      stateMap[i] = {
        startLine: 0,
        log: {
          time: null,
          message: null
        }
      }
    })

    const fileDirs = files.map(file=> `./logFiles/${file}`);

    const rec = ()=>{
      const promiseArray = Object.keys(stateMap).map(key=>{
        if(stateMap[key].log.time === null) {
          return readLine(fileDirs[key], stateMap[key].startLine, key)
        }
      })
      
      Promise.all(promiseArray)
      .then(updatedLogs=> {
        updatedLogs.forEach(updatedLog=>{
          if(updatedLog) {
            if(updatedLog.end) {
              delete stateMap[updatedLog.key];
              if(Object.keys(stateMap).length === 0) {
                console.log('done');
                return
              }
            } else {
              const key = updatedLog.key;
              
              stateMap[key].log = updatedLog.log;
              stateMap[key].startLine = updatedLog.newStartLine;
              
              // add log to min heap
              heap.push(stateMap[key])
            }
          }
        })

        if(Object.keys(stateMap).length === 0) {
          console.log('timeElapsed', Date.now() - startTime)
          console.log('done');
          return
        }
        const logToPrint = heap.pop()
        console.log('resultlog',logToPrint.log)
        logToPrint.log.time = null;
        return rec()
      })
      .catch(err=> console.log(err))
    }
    rec()
  })
}
startProcess()

const createTestFiles = ()=>{
  for(let fileNum = 0; fileNum < 10; fileNum++) {
    let wstream = fs.createWriteStream(`./logFiles/log${fileNum}.txt`)
    for(let i = 0; i<1000; i++) {
      let obj = `{"time":${i}, "message":"file${fileNum}_${i.toString()}"}\n`
      wstream.write(obj)
    } 
  }
}
// createTestFiles()

