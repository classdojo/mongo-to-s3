var fs          = require("fs");
var PassThrough = require("stream").PassThrough;
var inherits    = require("util").inherits;
var once        = require("once");

var EOF_AFTER = 500;
var START     = 0;
/* 
 *  @param fileName {String}
 *  @param options
 *    options.start {Number} Byte to start reading from (inclusive)
 *    options.EOFAfter: {Number} Emit EOF after N seconds of no new data.
 *    
*/
inherits(FsTail, PassThrough);
function FsTail(fileName, options) {
  PassThrough.call(this);
  options = options || {};
  this.__fileName    = fileName;
  this.__start       = options.start || START;
  this.__EOFAfter    = options.EOFAfter || EOF_AFTER;
}

FsTail.prototype.init = function() {
  this.__lastReadTime = new Date();
  this._allowStatOnce();
  this._allowEOFOnce();
  this._stat();
};

FsTail.prototype._statMain_ = function() {
  var me = this;
  fs.stat(this.__fileName, function(err, stat) {
    if(err) {
      return me.emit("error", err);
    }
    if(stat.size > me.__start) {
      me.__lastReadTime = new Date();
      me._teardownChangeListeners();
      me._readFileChunk(stat);
    } else {
      if(new Date() - me.__lastReadTime > me.__EOFAfter) {
        me._emitEOF();
      }
      me._allowStatOnce();
    }
  });
};

FsTail.prototype._allowStatOnce = function() {
  this._stat = once(this._statMain_.bind(this));
};

FsTail.prototype._allowEOFOnce = function() {
  this._emitEOF = once(this.emit.bind(this, "EOF"));
};

FsTail.prototype._readFileChunk = function(stat) {
  this.__lastReadTime    = new Date();
  this.__lastFileStat    = stat;
  this.__fileChunkStream = fs.createReadStream(this.__fileName, {start: this.__start, end: stat.size - 1});
  this.__start           = stat.size;
  this._allowEOFOnce();
  this.__fileChunkStream.pipe(this);
};

FsTail.prototype.end = function() {
  this.__fileChunkStream.unpipe(this);
  //allow stats again.
  this._allowStatOnce();
  this._setupChangeListeners();
};

FsTail.prototype._serviceChangeEvent = function() {
  this._stat();
};

/*
 * Setup both fs.watch and setInterval to queue up
 * change stat event.
*/
FsTail.prototype._setupChangeListeners = function() {
  this.__changeListenerTime = new Date();
  fs.watch(this.__fileName, this._serviceChangeEvent.bind(this));
  this.__i = setInterval(this._serviceChangeEvent.bind(this), 100);
};

/*
  Can be called when no change listeners are running. Happens
  on initial read of file.
*/
FsTail.prototype._teardownChangeListeners = function() {
  fs.unwatchFile(this.__fileName);
  if(this.__i) {
    clearInterval(this.__i);
    this.__i = null;
  }
};

FsTail.prototype.cleanUp = function(cb) {
  /* TODO: Ensure there's no outstanding stat call. */
  this._teardownChangeListeners();
  cb();
};

module.exports = function(fileName, options) {
  var tail = new FsTail(fileName, options);
  tail.init();
  return tail;
};

//export FsTail for testing.
module.exports.__FsTail = FsTail;
