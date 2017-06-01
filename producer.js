
var STOCK_PRICES = [
	 {"tickerSymbol":"AAPL" ,"price":119.72}
	,{"tickerSymbol":"XOM"  ,"price":91.56}
	,{"tickerSymbol":"GOOG" ,"price":527.83}
	,{"tickerSymbol":"BRK.A","price":223999.88}
	,{"tickerSymbol":"MSFT" ,"price":42.36}
	,{"tickerSymbol":"WFC"  ,"price":54.21}
	,{"tickerSymbol":"JNJ"  ,"price":99.78}
	,{"tickerSymbol":"WMT"  ,"price":85.91}
	,{"tickerSymbol":"CHL"  ,"price":66.96}
	,{"tickerSymbol":"GE"   ,"price":24.64}
	,{"tickerSymbol":"NVS"  ,"price":102.46}
	,{"tickerSymbol":"PG"   ,"price":85.05}
	,{"tickerSymbol":"JPM"  ,"price":57.82}
	,{"tickerSymbol":"RDS.A","price":66.72}
	,{"tickerSymbol":"CVX"  ,"price":110.43}
	,{"tickerSymbol":"PFE"  ,"price":33.07}
	,{"tickerSymbol":"FB"   ,"price":74.44}
	,{"tickerSymbol":"VZ"   ,"price":49.09}
	,{"tickerSymbol":"PTR"  ,"price":111.08}
	,{"tickerSymbol":"BUD"  ,"price":120.39}
	,{"tickerSymbol":"ORCL" ,"price":43.40}
	,{"tickerSymbol":"KO"   ,"price":41.23}
	,{"tickerSymbol":"T"    ,"price":34.64}
	,{"tickerSymbol":"DIS"  ,"price":101.73}
	,{"tickerSymbol":"AMZN" ,"price":370.56}
];

var nextNo = 0;

function getRandomTrade(){

	var nextDouble = Math.random();
	// 평균 가격의 편차 비율
    var MAX_DEVIATION = 0.2; // ie 20%
	//
	// 판매 거래 확률
    var PROBABILITY_SELL = 0.4; // ie 40%

	// 랜덤으로 리스트에서 한개 꺼내 온다.
	var stockPrice = STOCK_PRICES[Math.floor(nextDouble * STOCK_PRICES.length) ];

	var deviation = (nextDouble - 0.5) * 2.0 * MAX_DEVIATION;

	// 편차와 평균 가격을 설정
	var price = stockPrice.price * (1 + deviation);

	// 소수 2자리 까지
	stockPrice.price = Math.round(price * 100.0) / 100.0;

	// 판매 확율에 의한 구입, 판매 설정
	if (nextDouble < PROBABILITY_SELL) {
		stockPrice.tradeType = 1; // buy
	}else{
		stockPrice.tradeType = 0; // sell
	}

	// 최대 랜덤 주식수
    var MAX_QUANTITY = 10000;

	// 랜덤 주식 수량 설정
	stockPrice.quantity = Math.floor(nextDouble * MAX_QUANTITY) + 1;
	
	// 순차적 증가 아이디 번호값
	stockPrice.id = ++nextNo;

	return stockPrice;
}

var AWS = require('aws-sdk');

// TODO 서울 리전
AWS.config.region = 'ap-northeast-2';

AWS.config.credentials.get(function(err) {
    // 인증에 문제가 있다면 
    if (err) {
        console.log('Error retrieving credentials.');
        console.error(err);
        return;
    }

	var recordData = [];

	// 0~1초 사이 불규칙 데이터 생성 함수
	var putRandomTrade = function(){
		var stockPrice = getRandomTrade();
		// 키네시스 레코드 생성
		var record = {
			Data: JSON.stringify(stockPrice), // 필수
			PartitionKey: 'partition-' + stockPrice.id // 필수, 최대 256자
		};
		recordData.push(record);

		setTimeout(putRandomTrade, Math.floor(Math.random() * 1000)); // 0~1초 사이 랜덤값
	};

	// 함수 최초 실행, setTimeout으로 무한 재귀 함수 호출 
	putRandomTrade();

	// TODO delete Test
    //setInterval(function() {
	//	console.log(recordData);
	//	recordData = [];
	//	console.log('##################################################');
	//}, 1000);
	//return;

    // 키네시스 객체 생성
    var kinesis = new AWS.Kinesis({
        apiVersion: '2013-12-02'
    });
	
	// 키네시스 스트림에 1초에 한번씩 업로드
    setInterval(function() {
        if (!recordData.length) {
            return;
        }
		// 키네시스 업로드,  최대 500개 레코드 까지 처리 가능, 1레코드 최대 1MB , 전체레코드 최대 5MB 가능
        kinesis.putRecords({
            Records: recordData,
			// TODO 키네시스 스트림 이름
            StreamName: 'StockTradeStream' // 필수
        }, function(err, data) {
            if (err) {
                console.error(err);
            }
        });

		console.log(recordData);
		console.log('##################################################');
        // 버퍼 초기화
        recordData = [];

		// 1초
    }, 1000); 
});

