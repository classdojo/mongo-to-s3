var Stream = require('stream');


module.exports = function() {
  var buffer = '';
  var transform = new Stream.Transform({objectMode: true});
  
  transform._transform = function(data, encoding, done) {
    buffer += data || '';
    var split = buffer.split('\n');

    for(var i = 0; i < split.length - 1; i++) {
      var object = null;
      try {
        object = JSON.parse(split[i]);
      }
      catch(e) {
        console.log(e.toString() + ': ' + split[i]);
      }

      if(!!object) {
        transform.push(object);
      }
  	}

    buffer = split[split.length - 1];
    done();
  };

  transform._flush = function(done) {
    transform._transform('\n', null, done);
  };

  return transform;
};