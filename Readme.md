flume-inhale
---

```
const inhale = require('flume-inhale')
inhale(conf, db, stream, cb)
```

Where db is a flumedb instance, stream is a function that returns a pull-stream source providing the data to be written to db, and conf looks like this:

```
{
  update_interval: {minutes: 15},
  startday: '2022-01-01',
  minage: {days: 1},
  maxSpanPerReq: {months: 1},
  minSpanPerReq: {days: 1},
  tz: 'Europe/Berlin'
}
```

`minAge` specifies how long the data needs to be present in the source before being requested by flume-inhale. Sometimes sources will mutate entries for a while before they enter an immutable state, that's what minAge is for. (think of it as a safety margin, time-wise)

(see luxon for the time formats)

The user-provided stream function's signature is:

```
stream(dt_start, dt_end)
```

The arguments define a time span for the data to be returned (data sources must have monotonic timestamps!). `dt_start` and `dt_end` are instances of luxon DateTime objects. The stream must return data **before** dt_end, dt_end is exclusice! (dt_start <= data.timestamp < dt_end.

The last object in the pull-stream returned by stream() must be:

```
{
  type: '__since',
  data: {
    timestamp: _maxDate.setZone(conf.tz).toMillis()
  }
}
```

(Note to self: this gotta change)
