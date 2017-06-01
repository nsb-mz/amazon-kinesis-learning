console.log('Lambda Loading event...');

var AWS = require('aws-sdk');

// TODO 서울 리전
AWS.config.region = 'ap-northeast-2';

AWS.config.credentials.get(function(err) {
	// 인증에 문제가 있다면 
	if (err) {
//		console.log('Error retrieving credentials.');
		console.error(err);
		return;
	}

	var s3 = new AWS.S3();

	// 람다 햄들러 함수
	exports.handler = function(event, context) {
		console.log(JSON.stringify(event, null, '  '));
		// event json 객체 내부 구조는 이렇게 생겼습니다.
		//{
		//    "Records": [
		//        {
		//            "kinesis": {
		//                "kinesisSchemaVersion": "1.0",
		//                "partitionKey": "partition-34",
		//                "sequenceNumber": "49573724526609919986678056440148448191059958665534504962",
		//                "data": "eyJ0aWNrZXJTeW1ib2wiOiJQVFIiLCJwcmljZSI6MTM0LjAzLCJ0cmFkZVR5cGUiOjAsInF1YW50aXR5Ijo3NDY4LCJpZCI6MzR9",
		//                "approximateArrivalTimestamp": 1496218710.223
		//            },
		//            "eventSource": "aws:kinesis",
		//            "eventVersion": "1.0",
		//            "eventID": "shardId-000000000000:49573724526609919986678056440148448191059958665534504962",
		//            "eventName": "aws:kinesis:record",
		//            "invokeIdentityArn": "arn:aws:iam::??????????:role/LambdaPostsReaderRole",
		//            "awsRegion": "ap-northeast-2",
		//            "eventSourceARN": "arn:aws:kinesis:ap-northeast-2:??????????:stream/StockTradeStream"
		//        }
		//    ]
		//}


		// 레코드를 문자열로 변환해서 배열에 저장
		var arrBuffer = [];
		for(var i = 0, s = event.Records.length; i < s; ++i) {
			var data = event.Records[i].kinesis.data;
			var text = new Buffer(data, 'base64').toString('ascii');
			arrBuffer.push(text);
		}

		// 로그 확인
		//console.log("Decoded Payload: " + arrBuffer.join(''));

		var lastKey = event.Records[event.Records.length-1].kinesis.partitionKey;
		lastKey = lastKey.substring(lastKey.indexOf('-'));

		// S3에 저장
		s3.putObject({
			Bucket: 'bb-kinesis-to-s3', //  필수, TODO bb문자 자신이 S3에 생성했던 버킷 이름으로 대체
			Key: event.Records[0].kinesis.partitionKey 
					+ lastKey, // 필수, 고유 키 이름
			Body: arrBuffer.join('') // 배열에 있는 데이터 합치기
			}, function(err, data){
				if (err){
					console.error(err);	
				}
				context.done(null, "Done"); // 람다 완료
			});
	};
});

