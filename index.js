//jshint -W033
//jshint -W018
//jshint  esversion: 11

const pull = require('pull-stream')
const pullLooper = require('pull-looper')
const bufferUntil = require('pull-buffer-until')

const { DateTime } = require('luxon')
const { inspect } = require('util')
const { join } = require('path')

const Reduce = require('flumeview-reduce')

const defaults = {
  //stream_delay: {seconds: 1},
  update_interval: {minutes: 15},
  startday: '2022-01-01',
  minage: {days: 1},
  maxSpanPerReq: {months: 1},
  minSpanPerReq: {days: 1},
  tz: 'Europe/Berlin'
}

module.exports = function(conf, db, stream, cb) {
  conf = Object.assign({}, defaults, conf)
  //if (conf.data_dir) return cb(new Error('Need data_dir'))

  db.use('continuation', Reduce(1, (acc, item) => {
    if (item.type !== '__since') return acc
    return Math.max(acc || 0, item.data.timestamp)
  }))
   
  db.continuation.get((err, value) => {
    if (err) {
      console.error('Unable to read continuation value:', err.message)
      return cb(err)
    }
    console.error('continuation value is', formatTimestamp(value))
    conf.continuation = value
    
    function periodic() {
      update(db, conf, (err, continuation)=>{
        conf.continuation = continuation
        if (err) {
          console.error('-- STREAM ABORT')
          console.error(inspect(err, {depth: 6, colors: true}))
          return cb(err)
        } else {
          console.error('flumedb is in sync with upstream source.')
        }
        const ms = DateTime.fromSeconds(0).plus(conf.update_interval).toMillis()
        setTimeout(periodic, ms)
      })
    }

    periodic()
  })

  function update(db, conf, cb) {
    let continuation = conf.continuation

    const dt_max = DateTime.now().setZone(conf.tz).minus(conf.minage)
    const dt_start = conf.continuation ?
      DateTime.fromSeconds(conf.continuation / 1000).setZone(conf.tz) :    
      DateTime.fromISO(conf.startday).setZone(conf.tz).startOf('day')

    const dt_end = DateTime.min(dt_max, dt_start.plus(conf.maxSpanPerReq))

    const wait = dt_start.plus(conf.minSpanPerReq).toSeconds() - dt_end.toSeconds()
    if (wait>0) {
      console.error(`It is too early, will retry ${DateTime.now().plus({seconds: wait}).toRelative()}`)
      setTimeout( ()=>{
        cb(null, continuation)
      }, wait * 1000)
      return 
    }   

    pull(
      stream(dt_start, dt_end, continuation),
      pullLooper,
      bufferUntil( buff=>{
        return buff[buff.length-1].type == '__since'
      }),
      pull.asyncMap( (items, cb) => {
        //console.log(items)
        db.append(items, (err, seq) => {
          if (err) return cb(err)
          db.continuation.get((err, value) => {
            if (err) return cb(err)
            console.error('new continuation value:', formatTimestamp(value))
            continuation = value
            cb(null, seq)
          })
        })
      }),
      pull.onEnd(err=>{
        cb(err, continuation)
      })
    )
  }

  function formatTimestamp(ts) {
    const t = DateTime.fromSeconds(ts/1000).setZone(conf.tz)
    return t.setLocale('de').toLocaleString(DateTime.DATETIME_MED)
  }
}

