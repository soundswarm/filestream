const testFolder = './logFiles';
const fsPromise = require('fs-promise')
const fs = require ('fs')

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
    const _2117 = 78099676155333360000; //a date far in the future
    let minTime = {key: null, time:_2117};

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
            }
          }
        })

        Object.keys(stateMap).forEach(key=>{
          if(stateMap[key].log.time < minTime.time) {
            minTime.key = key;
            minTime.time = stateMap[key].log.time;
          }
        })
        if(Object.keys(stateMap).length === 0) {
          console.log('timeElapsed', Date.now() - startTime)
          console.log('done');
          return
        }
        console.log('resultlog',stateMap[minTime.key].log, minTime.key)
        stateMap[minTime.key].log.time = null;
        minTime.time = _2117;
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
    for(let i = 0; i<100; i++) {
      let obj = `{"time":${i}, "message":"file${fileNum}_${i.toString()}"}\n`
      wstream.write(obj)
    } 
  }
}

