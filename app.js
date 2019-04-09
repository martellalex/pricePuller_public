const csv = require('csv-parser');
const fs = require('fs');
const _ = require('underscore');
const BitMEXClient = require('bitmex-realtime-api');
const client = new BitMEXClient();
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csvWriter = createCsvWriter({
	path: 'out.csv',
	header: [
		{id:'ticker', title: 'Ticker'},
		{id:'api', title: 'API'},
		{id:'price', title: 'Price'}
	]
});
const request = require('request');

var results = [];

//read tickers from priceInputs.csv
fs.createReadStream('priceInputs.csv')
	.pipe(csv())
	.on('data', (data) => results.push(data))
	.on('end', () => {

		//sort tickers by exchange and create arrays of tickers for each exchange
	var bitmexArray = _.where(results,{api:'bitmex'});
	var bitfinexArray = _.where(results,{api:'bitfinex'});
	var binanceArray = _.where(results,{api:'binance'});
	var coingeckoArray = _.where(results,{api:'coingecko'});
	var coinbaseArray = _.where(results,{api:'coinbase'});

	//this array we'll use to route tickers to each exchange connector
	var megaArray = [bitmexArray,bitfinexArray,binanceArray,coingeckoArray,coinbaseArray];
	var flatMegaArray = _.flatten(megaArray);
	var flatResults = _.flatten(results);
	var differenceArray = _.difference(flatResults,flatMegaArray);
	megaArray.push(differenceArray);

	//for each array, go fetch the data and then write it to csv
	Promise.all(megaArray.map(function(element){
				var apiPromiseArray = element.length > 0 ? quotePromiseArray(element) : [];
				return Promise.all(apiPromiseArray)
			})
		)
		.then(function(values){
			var flat = _.flatten(values);
			// console.log(flat);
			csvWriter
				.writeRecords(flat)
				.then(() => {console.log('CSV written. Return to excel and click ok');process.exit() } );
		})
	});

//connector for Bitmex
var bitmexQuote = function(element){
	return new Promise(
		function(resolve,reject){
			var quote= []		
			var ticker = element.ticker;
			var exchange = element.api;
			client.addStream(ticker,'quote',function(data,symbol,table){
				const latestQuote = data[data.length - 1];
				quote.push(latestQuote);
				if(quote.length == 1){
					var ask = quote[0].askPrice;
					var bid = quote[0].bidPrice;
					var price = (ask + bid) /2;
					var priceQuote = {};
					priceQuote['ticker'] = ticker;
					priceQuote['api'] = exchange;
					priceQuote['price'] = price;
					resolve(priceQuote);
				}
			})
		}
	)
}

//connector for bitfinex
var bitfinexQuote = function(element){
	return new Promise(
		function(resolve,reject){
			var ticker = 't'+element.ticker
			var url = 'https://api-pub.bitfinex.com/v2/tickers?symbols='+ticker;
			request(url,function(error,response,body){
				var parsedBody = JSON.parse(body);
				var priceQuote = {};
				priceQuote['ticker'] = element.ticker;
				priceQuote['api'] = element.api;
				var flatBody = _.flatten(parsedBody);
				priceQuote['price'] = flatBody[7];
				resolve(priceQuote)
			})
		}
	)
}

// connector for binance
var binanceQuote = function(element){
	return new Promise(
		function(resolve,reject){
			var ticker = element.ticker
			var url = 'https://api.binance.com/api/v3/ticker/price?symbol='+ticker;
			request(url,function(error,response,body){
				var parsedBody = JSON.parse(body);
				var priceQuote = {};
				priceQuote['ticker'] = element.ticker;
				priceQuote['api'] = element.api;
				priceQuote['price'] = parsedBody['price'];
				resolve(priceQuote)
			})
		}
	)
}

//connector for coingecko
var coingeckoQuote = function(element){
	return new Promise(
		function(resolve,reject){
			var lowerCase = element.ticker.toLowerCase();
			var coinId = lowerCase.split('_')[0];
			var basePair = lowerCase.split('_')[1];
			var url = 'https://api.coingecko.com/api/v3/coins/'+coinId;
			request(url,function(error,response,body){
				var parsedBody = JSON.parse(body);
				var priceQuote = {};
				priceQuote['ticker'] = element.ticker;
				priceQuote['api'] = element.api;
				priceQuote['price'] = parsedBody['market_data']['current_price'][basePair];
				resolve(priceQuote)
			})
		}
	)
}

// connector for coinbase
var coinbaseQuote = function(element){
	return new Promise(
		function(resolve,reject){
			var ticker = element.ticker
			var url = 'https://api.coinbase.com/v2/prices/'+ticker+'/buy'
			request(url,function(error,response,body){
				var parsedBody = JSON.parse(body);
				var priceQuote = {};
				priceQuote['ticker'] = element.ticker;
				priceQuote['api'] = element.api;
				priceQuote['price'] = parsedBody['data']['amount'];
				resolve(priceQuote)
			})
		}
	)
}

// connector not supported
var notSupported = function(element){
	return new Promise(
		function(resolve,reject){
			var priceQuote = {};
		  	priceQuote['ticker'] = element.ticker;
			priceQuote['api'] = element.api;
			priceQuote['price'] = 'Sorry, this api is not supported';			
			resolve(priceQuote);
		}
	)
}

//take the array of tickers for each exchange, and depending on which exchange it's for, route it to the appropriate connector
var quotePromiseArray = function(array){
	console.log(array)
	var api = array.length > 0 ? array[0].api : 'other';
	if(api == 'bitmex'){
		var promiseArray = _.map(array,function(element){
			return bitmexQuote(element);
		});
		return promiseArray;
	}
	if(api == 'bitfinex'){
		var promiseArray = _.map(array,function(element){
			return bitfinexQuote(element);
		});
		return promiseArray;		
	}
	if(api == 'binance'){
		var promiseArray = _.map(array,function(element){
			return binanceQuote(element);
		});
		return promiseArray;		
	}
	if(api == 'coingecko'){
		var promiseArray = _.map(array,function(element){
			return coingeckoQuote(element);
		});
		return promiseArray;		
	}
	if(api == 'coinbase'){
		var promiseArray = _.map(array,function(element){
			return coinbaseQuote(element);
		});
		return promiseArray;		
	}
	else {
		var promiseArray = _.map(array,function(element){
			return notSupported(element);
		});
		return promiseArray;		
	}	
}