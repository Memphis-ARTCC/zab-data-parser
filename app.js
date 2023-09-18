import mongoose from 'mongoose';
import axios from 'axios';
import dotenv from 'dotenv';
import schedule from 'node-schedule';
import inside from 'point-in-polygon';
import Redis from 'ioredis';


import AtcOnline from './models/AtcOnline.js';
import PilotOnline from './models/PilotOnline.js';
import Pireps from './models/Pireps.js';
import ControllerHours from './models/ControllerHours.js';

dotenv.config();

const redis = new Redis(process.env.REDIS_URI);

redis.on('error', err => { throw new Error(`Failed to connect to Redis: ${err}`); });
redis.on('connect', () => console.log('Successfully connected to Redis'));

const zabApi = axios.create({
	baseURL: process.env.ZAB_API_URL,
	headers: {
		'Authorization': `Bearer ${process.env.ZAB_API_KEY}`
	}
});

const atcPos = ["BNA", "FSM", "HSV", "JAN", "LIT", "CBM", "HOP", "NMM", "ASG", "CGI", "EOD", "FYV", "GLH", "GTR", "GWO", "HKS", "HUA", "JWN", "MEI", "MEM", "MKL", "MQY", "NJW", "NQA", "OLV", "PAH", "ROG", "TUP", "XNA"];
const airports = ["KBNA", "KFSM", "KHSV", "KJAN", "KLIT", "KCBM", "KHOP", "KNMM", "KASG", "KCGI", "KEOD", "KFYV", "KGLH", "KGTR", "KGWO", "KHKS", "KHUA", "KJWN", "KMEI", "KMEM", "KMKL", "KMQY", "KNJW", "KNQA", "KOLV", "KPAH", "KROG", "KTUP", "KXNA"];
const neighbors = ["ATL", "ZHU", "FTW", "IND", "KC"];

const airspace = [
	[-95.6125, 36.016667],
	[-95.4, 36.204167],
	[-95.195833, 36.2875],
	[-94.683333, 36.433333],
	[-94.408333, 36.491667],
	[-93.25, 36.725],
	[-90.566667, 37.15],
	[-88.833333, 37.533333],
	[-88.316667, 37.725],
	[-87.397222, 37.275],
	[-86.15, 37.3],
	[-85.745833, 37.013889],
	[-85.583333, 36.9],
	[-85.4, 36.183333],
	[-85.283333, 35.65],
	[-86.0, 35.516667],
	[-87.0, 35.333333],
	[-87.0, 34.366667],
	[-87.25, 34.1],
	[-87.55, 34.016667],
	[-87.633333, 33.316667],
	[-87.986111, 33.043056],
	[-87.85, 32.697222],
	[-88.347222, 32.330556],
	[-88.325, 31.516667],
	[-88.9, 31.547222],
	[-89.85, 31.630556],
	[-90.345833, 31.65],
	[-91.308333, 31.9125],
	[-91.688889, 32.283333],
	[-91.9, 33.004167],
	[-93.158333, 33.95],
	[-93.541667, 34.033333],
	[-94.533333, 34.533333],
	[-94.783333, 34.691667],
	[-95.0, 35.066667],
	[-95.0, 35.383333],
	[-95.0, 35.654167],
	[-95.6125, 36.016667]
];

mongoose.set('toJSON', { virtuals: true });
mongoose.connect(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
const db = mongoose.connection;
db.once('open', () => console.log('Successfully connected to MongoDB'));

const pollVatsim = async () => {
	await AtcOnline.deleteMany({}).exec();
	await PilotOnline.deleteMany({}).exec();

	console.log("Fetching data from VATSIM.");
	const { data } = await axios.get('https://data.vatsim.net/v3/vatsim-data.json');

	// PILOTS

	const dataPilots = [];

	let redisPilots = await redis.get('pilots');
	redisPilots = (redisPilots && redisPilots.length) ? redisPilots.split('|') : [];

	for (const pilot of data.pilots) { // Get all pilots that depart/arrive in ARTCC's airspace
		if (pilot.flight_plan !== null && (airports.includes(pilot.flight_plan.departure) || airports.includes(pilot.flight_plan.arrival) || inside([pilot.latitude, pilot.longitude], airspace))) {
			await PilotOnline.create({
				cid: pilot.cid,
				name: pilot.name,
				callsign: pilot.callsign,
				aircraft: pilot.flight_plan.aircraft_faa,
				dep: pilot.flight_plan.departure,
				dest: pilot.flight_plan.arrival,
				code: Math.floor(Math.random() * (999 - 101) + 101),
				lat: pilot.latitude,
				lng: pilot.longitude,
				altitude: pilot.altitude,
				heading: pilot.heading,
				speed: pilot.groundspeed,
				planned_cruise: pilot.flight_plan.altitude.includes("FL") ? (pilot.flight_plan.altitude.replace("FL", "") + '00') : pilot.flight_plan.altitude, // If flight plan altitude is 'FL350' instead of '35000'
				route: pilot.flight_plan.route,
				remarks: pilot.flight_plan.remarks
			});

			dataPilots.push(pilot.callsign);

			redis.hmset(`PILOT:${pilot.callsign}`,
				'callsign', pilot.callsign,
				'lat', `${pilot.latitude}`,
				'lng', `${pilot.longitude}`,
				'speed', `${pilot.groundspeed}`,
				'heading', `${pilot.heading}`,
				'altitude', `${pilot.altitude}`,
				'cruise', `${pilot.flight_plan.altitude.includes("FL") ? (pilot.flight_plan.altitude.replace("FL", "") + '00') : pilot.flight_plan.altitude}`,
				'destination', `${pilot.flight_plan.arrival}`,
			);
			redis.expire(`PILOT:${pilot.callsign}`, 300);
			redis.publish('PILOT:UPDATE', pilot.callsign);

		}
	}

	for (const pilot of redisPilots) {
		if (!dataPilots.includes(pilot)) {
			redis.publish('PILOT:DELETE', pilot);
		}
	}

	redis.set('pilots', dataPilots.join('|'));
	redis.expire(`pilots`, 65);

	// CONTROLLERS
	const dataControllers = [];
	let redisControllers = await redis.get('controllers');
	redisControllers = (redisControllers && redisControllers.length) ? redisControllers.split('|') : [];

	const dataNeighbors = [];

	for (const controller of data.controllers) { // Get all controllers that are online in ARTCC's airspace
		if (atcPos.includes(controller.callsign.slice(0, 3)) && controller.callsign !== "PRC_FSS" && controller.facility !== 0) {
			await AtcOnline.create({
				cid: controller.cid,
				name: controller.name,
				rating: controller.rating,
				pos: controller.callsign,
				timeStart: controller.logon_time,
				atis: controller.text_atis ? controller.text_atis.join(' - ') : '',
				frequency: controller.frequency
			});

			dataControllers.push(controller.callsign);

			const session = await ControllerHours.findOne({
				cid: controller.cid,
				timeStart: controller.logon_time
			});

			if (!session) {
				await ControllerHours.create({
					cid: controller.cid,
					timeStart: controller.logon_time,
					timeEnd: new Date(new Date().toUTCString()),
					position: controller.callsign
				});
				await zabApi.post(`/stats/fifty/${controller.cid}`);
			} else {
				session.timeEnd = new Date(new Date().toUTCString());
				await session.save();
			}
		}
		const callsignParts = controller.callsign.split('_');
		if (neighbors.includes(callsignParts[0]) && callsignParts[callsignParts.length - 1] === "CTR") { // neighboring center
			dataNeighbors.push(callsignParts[0]);
		}
	}

	for (const atc of redisControllers) {
		if (!dataControllers.includes(atc)) {
			redis.publish('CONTROLLER:DELETE', atc);
		}
	}

	redis.set('controllers', dataControllers.join('|'));
	redis.expire(`controllers`, 65);
	redis.set('neighbors', dataNeighbors.join('|'));
	redis.expire(`neighbors`, 65);

	// METARS

	const airportsString = airports.join(","); // Get all METARs, add to database
	const response = await axios.get(`https://metar.vatsim.net/${airportsString}`);
	const metars = response.data.split("\n");

	for (const metar of metars) {
		redis.set(`METAR:${metar.slice(0, 4)}`, metar);
	}

	// ATIS

	const dataAtis = [];
	let redisAtis = await redis.get('atis');
	redisAtis = (redisAtis && redisAtis.length) ? redisAtis.split('|') : [];

	for (const atis of data.atis) { // Find all ATIS connections within ARTCC's airspace
		const airport = atis.callsign.slice(0, 4);
		if (airports.includes(airport)) {
			dataAtis.push(airport);
			redis.expire(`ATIS:${airport}`, 65);
		}
	}

	for (const atis of redisAtis) {
		if (!dataAtis.includes(atis)) {
			redis.publish('ATIS:DELETE', atis);
			redis.del(`ATIS:${atis}`);
		}
	}

	redis.set('atis', dataAtis.join('|'));
	redis.expire(`atis`, 65);
};

const getPireps = async () => {
	console.log('Fetching PIREPs.');
	let twoHours = new Date();
	twoHours = new Date(twoHours.setHours(twoHours.getHours() - 2));

	await Pireps.deleteMany({ $or: [{ manual: false }, { reportTime: { $lte: twoHours } }] }).exec();

	const pirepsJson = await axios.get('https://www.aviationweather.gov/cgi-bin/json/AirepJSON.php');
	const pireps = pirepsJson.data.features;
	for (const pirep of pireps) {
		if ((pirep.properties.airepType === 'PIREP' || pirep.properties.airepType === 'Urgent PIREP') && inside(pirep.geometry.coordinates.reverse(), airspace) === true) { // Why do you put the coordinates the wrong way around, FAA? WHY?
			const wind = `${(pirep.properties.wdir ? pirep.properties.wdir : '')}${pirep.properties.wspd ? '@' + pirep.properties.wspd : ''}`;
			const icing = ((pirep.properties.icgInt1 ? pirep.properties.icgInt1 + ' ' : '') + (pirep.properties.icgType1 ? pirep.properties.icgType1 : '')).replace(/\s+/g, ' ').trim();
			const skyCond = (pirep.properties.cloudCvg1 ? pirep.properties.cloudCvg1 + ' ' : '') + (pirep.properties.Bas1 ? ('000' + pirep.properties.Bas1).slice(-3) : '') + (pirep.properties.Top1 ? '-' + ('000' + pirep.properties.Top1).slice(-3) : '');
			const turbulence = (pirep.properties.tbInt1 ? pirep.properties.tbInt1 + ' ' : '') + (pirep.properties.tbFreq1 ? pirep.properties.tbFreq1 + ' ' : '') + (pirep.properties.tbType1 ? pirep.properties.tbType1 : '').replace(/\s+/g, ' ').trim();
			try {
				await Pireps.create({
					reportTime: pirep.properties.obsTime || '',
					location: pirep.properties.rawOb.slice(0, 3) || '',
					aircraft: pirep.properties.acType || '',
					flightLevel: pirep.properties.fltlvl || '',
					skyCond: skyCond,
					turbulence: turbulence,
					icing: icing,
					vis: pirep.visibility_statute_mi ? pirep.visibility_statute_mi._text : '',
					temp: pirep.properties.temp ? pirep.properties.temp : '',
					wind: wind,
					urgent: pirep.properties.airepType === 'Urgent PIREP' ? true : false,
					raw: pirep.properties.rawOb,
					manual: false
				});
			} catch (e) {
				console.log(e);
			}
		}
	}
};


(async () => {
	await redis.set('airports', airports.join('|'));
	await pollVatsim();
	await getPireps();
	schedule.scheduleJob('*/15 * * * * *', pollVatsim); // run every 15 seconds
	schedule.scheduleJob('*/2 * * * *', getPireps); // run every 2 minutes
})();


//https://www.aviationweather.gov/adds/dataserver_current/httpparam?dataSource=aircraftreports&requestType=retrieve&format=xml&minLat=30&minLon=-113&maxLat=37&maxLon=-100&hoursBeforeNow=2
//https://www.aviationweather.gov/cgi-bin/json/AirepJSON.php