var MongoClient = require('mongodb').MongoClient;
var request = require('request');
var fs = require('fs');
var zlib = require('zlib');
var path = require('path');
var async = require('async');

var con_url = 'mongodb://127.0.0.1:27017/testdb';

var tileUrl = 'https://{f}.tiles.mapbox.com/v4/mapbox.mapbox-streets-v6/{z}/{x}/{y}.vector.pbf?access_token=pk.eyJ1IjoiMTc0OTU4MjU5OSIsImEiOiI4Yzk5MTg0YTlhM2IzM2JkNzEyYmFhZTA3NjI4NjdhOSJ9.JKwJtLmL9BaTo1cgZDTg_Q';

var startZ = 0;
var endZ = 10;

//某一个level上的x数量
function numberXOfZ(z) {
	return 1 << z;
}

function numberYOfZ(z) {
	return 1 << z;
}
//所有瓦片数量
function allCount() {
	var sum = 0;
	for (var i = startZ; i <= endZ; i++) {
		var mx = numberXOfZ(i);
		var my = mx;
		sum += mx * my;
	}
	return sum;
}
var ac = allCount();

console.log('本任务一共有: ', ac, ' 个瓦片');

//利用async的queue完成任务队列，取每个任务然后下载
var task_queue = async.queue(function(task, callback) {
	task.download(callback);
}, ac/8);

var globalCount = 0;

MongoClient.connect(con_url, function(err, db) {
	if (err) throw (err)

	var col = db.collection("mvts");
	for (var i = startZ; i <= endZ; i++) {
		var nox = numberXOfZ(i);
		var noy = numberYOfZ(i);
		for (var j = 0; j < nox; j++) {
			for (var k = 0; k < noy; k++) {
				(function(z, x, y) {
					col.findOne({
						z: z,
						x: x,
						y: y
					}, {
						limit: 1
					}, function(e, doc) {
						if (!doc) {
							//往任务队列里面塞任务
							task_queue.push({
								download: downloadFn(z, x, y, col)
							}, function(err) {
								if (!err)
									globalCount++;
								if (globalCount == ac)
									db.close();
							});
						} else {
							//如果已存在，则不用下载
							task_queue.push({
								download: function(){}
							}, function(err) {
								if (!err)
									globalCount++;
								if (globalCount == ac)
									db.close();
							});
						}
					})
				})(i, j, k)
			}
		}
	};
});

function random(arr) {
	var fz = ['a', 'b'];
	return Math.random() > 0.5 ? fz[0] : fz[1];
}

//记录下载失败的路径
function logFail(file, content) {
	fs.appendFile(path.join(__dirname, file), content + '\r\n');
}

//下载
function downloadFn(z, x, y, col) {
	return function(cb) {
		
		var url = tileUrl.replace('{z}', z).replace('{x}', x).replace('{y}', y).replace('{f}', random());
		var chunks = [];
		var size = 0;
		var gunzip = zlib.createGunzip();
		request(url)
			.on('error', function(e) {
				logFail('network.txt', z+"#"+x+'#'+y);
			})
			//原始数据是gzip的
			.pipe(gunzip)
			.on('error', function(e) {
				logFail('notfound.txt', z+"#"+x+'#'+y);
			})
			.on('data', function(chunk) {
				chunks.push(chunk);
				size += chunk.length;
				console.log('downloading ... ',z,x,y);
			})
			.on('end', function() {
				var buf = Buffer.concat(chunks, size);
				chunks = [];
				size = 0;
				//插入Mongodb
				col.insert({
					z: z,
					x: x,
					y: y,
					tile: buf
				}, null, function(e) {
					cb();
					console.log('download',z,x,y,'success');
				});

			})
	}
}