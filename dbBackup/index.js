const { MongoClient } = require("mongodb");

const Logger = console;

const collections = [
	{name: 'allocations'},
	{name: 'areas', complete: true},
	{name: 'assignees'},
	{name: 'buyerUnitList'},
	{name: 'buyerUnits'},
	{name: 'cities', complete: true},
	{name: 'compensation.structures', drop: true, complete: true},
	{name: 'configurations'},
	{name: 'conversations'},
	{name: 'counters'},
	{name: 'databaseMigrations', drop: true, complete: true},
	{name: 'dealActions'},
	{name: 'dealDocuments'},
	{name: 'dealProcesses'},
	{name: 'deals'},
	{name: 'demandDrafts', complete: true},
	{name: 'dialogs'},
	{name: 'documents', complete: true},
	{name: 'feedbacks'},
	{name: 'groups'},
	{name: 'incentives'},
	{name: 'invoices'},
	{name: 'jurisdictions', complete: true},
	{name: 'listings'},
	{name: 'listingDestinations', complete: true},
	{name: 'locationMaps', complete: true},
	{name: 'locations', complete: true},
	{name: 'notifications'},
	{name: 'projects'},
	{name: 'projectMaps'},
	{name: 'rawUnits'},
	{name: 'reports.buyers.visitCounts', complete: true},
	{name: 'requests'},
	{name: 'requirements'},
	{name: 'reviews'},
	{name: 'roads', complete: true},
	{name: 'role-assignment', complete: true},
	{name: 'roles', complete: true},
	{name: 'sellerProjects'},
	{name: 'sellerUnitList'},
	{name: 'sellerUnits'},
	{name: 'settings', complete: true},
	{name: 'sros', complete: true},
	{name: 'subTeams', complete: true},
	{name: 'templates', complete: true},
	{name: 'units'},
	{name: 'users'},
	{name: 'verifiedFields'},
	{name: 'visits'},
];

async function run({date, seconds} = {}) {
	const xDaysAgo = new Date(date.getTime() - 1000 * seconds);
	const dateQuery = {$gte: xDaysAgo, $lte: date};

	const uri = process.env.MONGO_URL;
	const client = new MongoClient(uri);

	try {
		const settlinDb = client.db('settlin');
		const backupDb = client.db('backup');

		Logger.debug('Starting backup from', xDaysAgo, 'to', date);

		const start = new Date();
		const time = () => ((new Date() - start) / 1000) + 's';

		const bulk = {}, settlin = {}, backup = {};
		
		for (let i = 0; i < collections.length; i++) {
			const {name: c, drop, complete} = collections[i];
			settlin[c] = settlinDb.collection(c);
			backup[c] = backupDb.collection(c);
			if (complete) {
				// if (drop) await backup[c].deleteMany({});
				bulk[c] = backup[c].initializeUnorderedBulkOp();
				settlin[c].find().forEach(d => bulk[c].find({_id: d._id}).upsert().replaceOne(d));
				continue;
			}

			try {
				await backup[c].drop();
			}
			catch (e) {
				//
			}
			if (!backup[c]) {
				Logger.debug(c + ': collection not found');
				return {error: c + ': collection not found'};
			}
			try {
				bulk[c] = backup[c].initializeUnorderedBulkOp();
			}
			catch (e) {
				Logger.error('collection', c, e);
			}
		}
		Logger.debug('Clean Collections:', time());


		// counters
		(await settlin.counters.find({updatedAt: dateQuery}).toArray()).forEach(i => bulk.counters.insert(i));

		// allocations etc
		(await settlin.allocations.find({createdAt: dateQuery}).toArray()).forEach(i => bulk.allocations.insert(i));
		(await settlin.notifications.find({createdAt: dateQuery}).toArray()).forEach(i => bulk.notifications.insert(i));
		(await settlin.reviews.find({$or: [
			{_id: 'average'},
			{updatedAt: {$gte: new Date(new Date().getTime() - 86400000 * 100)}},
		]}).toArray()).forEach(i => bulk.reviews.find({_id: i._id}).upsert().replaceOne(i));
		Logger.debug('Counters, Notifications, Reviews, Allocations:', time());

		let rawUnitIdsObj = {}, groupIdsObj = {}, indIdsObj = {}, unitIdsObj = {}, projectIdsObj = {}, requestIdsObj = {}, configIdsObj = {};
		// requests
		(await settlin.requests.find({createdAt: dateQuery}).toArray()).forEach(r => {
			requestIdsObj[r._id] = true;
			if (((r.info || {}).unit || {})._id) unitIdsObj[r.info.unit.id] = true;
			if (((r.info || {}).project || {})._id) projectIdsObj[r.info.project.id] = true;
			if ((r.group || {}).id) groupIdsObj[r.group.id] = true;
			bulk.requests.insert(r);
		});
		const requestIds = Object.keys(requestIdsObj);
		(await settlin.conversations.find({'request.id': {$in: requestIds}}).toArray()).forEach(i => bulk.conversations.insert(i));
		(await settlin.incentives.find({'doc.id': {$in: requestIds}}).toArray()).forEach(i => bulk.incentives.insert(i));
		Logger.debug('Requests:', time());

		// rawUnits tmp
		(await settlin.rawUnits.find({createdAt: {$gte: 7 * dateQuery.$gte, $lte: dateQuery.$lte}}).toArray()).forEach(r => {
			rawUnitIdsObj[r._id] = true;
			if ((r.processedUnit || {}).id) unitIdsObj[r.processedUnit.id] = true;
			if ((r.saleInfo.group || {}).id) groupIdsObj[r.saleInfo.group.id] = true;
		});
		// rawUnits to be worked on
		(await settlin.rawUnits.find({
			'assignedTo.userId': {'$in': [ '' ]},
			'cancellation.flag': false,
			'customer.phone.0': {'$exists': true},
			'processDetails.status': {'$in': [ 'init', 'pending', 'groupAssociated' ]},
		}, {limit: 500}).toArray()).forEach(r => {
			rawUnitIdsObj[r._id] = true;
			if ((r.processedUnit || {}).id) unitIdsObj[r.processedUnit.id] = true;
			if ((r.saleInfo.group || {}).id) groupIdsObj[r.saleInfo.group.id] = true;
		});

		// units tmp
		(await settlin.units.find({createdAt: dateQuery}, {_id: 1}).toArray()).forEach(u => { unitIdsObj[u._id] = true; });

		// buyers tmp
		(await settlin.groups.find({relationship: 'buyer', createdAt: {$gte: dateQuery}}).toArray()).forEach(g => {
			groupIdsObj[g._id] = true;
		});

		// assignees
		const gids = Object.keys(groupIdsObj);
		(await settlin.assignees.find({'doc.collection': 'buyers', 'doc.id': {$in: gids}}).toArray()).forEach(x => bulk.assignees.insert(x));
		(await settlin.groups.find({'_id': {$in: gids}}).toArray()).forEach(x => bulk.groups.insert(x));
		(await settlin.buyerUnitList.find({'buyer.id': {$in: gids}}).toArray()).forEach(x => bulk.buyerUnitList.insert(x));

		// buyerUnits
		const buyerUnitIds = [];
		(await settlin.buyerUnits.find({'group.id': {$in: gids}}).toArray()).forEach(b => {
			buyerUnitIds.push(b._id);
			if (b.unit.id) unitIdsObj[b.unit.id] = true;
			bulk.buyerUnits.insert(b);
		});
		(await settlin.deals.find({_id: {$in: buyerUnitIds}}).toArray()).forEach(i => {
			bulk.deals.insert(i);
		});
		(await settlin.dealActions.find({dealId: {$in: buyerUnitIds}}).toArray()).forEach(x => bulk.dealActions.insert(x));
		(await settlin.dealDocuments.find({dealId: {$in: buyerUnitIds}}).toArray()).forEach(x => bulk.dealDocuments.insert(x));
		(await settlin.dialogs.find({buyerUnitId: {$in: buyerUnitIds}}).toArray()).forEach(i => bulk.dialogs.insert(i));
		(await settlin.visits.find({buyerUnitId: {$in: buyerUnitIds}}).toArray()).forEach(i => bulk.visits.insert(i));
		(await settlin.incentives.find({'doc.id': {$in: buyerUnitIds}}).toArray()).forEach(i => bulk.incentives.insert(i));
		const invoiceIds = [];
		(await settlin.invoices.find({buyerUnitId: {$in: buyerUnitIds}}).toArray()).forEach(i => {
			invoiceIds.push(i._id);
			bulk.invoices.insert(i);
		});
		(await settlin.incentives.find({'doc.id': {$in: invoiceIds}}).toArray()).forEach(inc => bulk.incentives.insert(inc));
		Logger.debug('Buyers:', time());

		// units
		const unitIds = Object.keys(unitIdsObj);
		(await settlin.sellerUnits.find({'unit.id': {$in: unitIds}}).toArray()).forEach(su => bulk.sellerUnits.insert(su));
		(await settlin.sellerUnitList.find({'unit._id': {$in: unitIds}}).toArray()).forEach(su => bulk.sellerUnitList.insert(su));
		(await settlin.units.find({_id: {$in: unitIds}}).toArray()).forEach(u => {
			bulk.units.insert(u);
			if ((u.saleInfo.group || {}).id) groupIdsObj[u.saleInfo.group.id] = true;
			if (u.rawUnitId) rawUnitIdsObj[u.rawUnitId] = true;
			if ((u.project || {}).id) projectIdsObj[u.project.id] = true;
			if (u.configId) configIdsObj[u.configId] = true;
		});
		(await settlin.incentives.find({'doc.id': {$in: unitIds}}).toArray()).forEach(i => bulk.incentives.insert(i));
		(await settlin.visits.find({unitId: {$in: unitIds}, relationship: 'seller'}).toArray()).forEach(i => bulk.visits.insert(i));
		(await settlin.listings.find({'doc.collection': 'units', 'doc.id': {$in: unitIds}}).toArray()).forEach(i => bulk.listings.insert(i));
		(await settlin.units.find({parentUnitId: {$in: unitIds}}).toArray()).forEach(i => bulk.units.find({_id: i._id}).upsert().replaceOne(i));
		(await settlin.verifiedFields.find({'for.collection': 'units', 'for._id': {$in: unitIds}}).toArray()).forEach(i => bulk.verifiedFields.insert(i));
		(await settlin.feedbacks.find({unitId: {$in: unitIds}}).toArray()).forEach(i => bulk.feedbacks.insert(i));
		Logger.debug('Units:', time());

		// rawUnits
		const rawUnitIds = Object.keys(rawUnitIdsObj);
		(await settlin.rawUnits.find({_id: {$in: rawUnitIds}}).toArray()).forEach(i => bulk.rawUnits.insert(i));
		(await settlin.incentives.find({'doc.id': {$in: rawUnitIds}}).toArray()).forEach(i => bulk.incentives.insert(i));
		Logger.debug('RawUnits:', time());

		// groups
		const groupIds = Object.keys(groupIdsObj);
		(await settlin.groups.find({_id: {$in: groupIds}}).toArray()).forEach(g => {
			bulk.groups.insert(g);
			g.members.forEach(m => { indIdsObj[m.id] = true; });
		});
		(await settlin.requirements.find({'for.id': {$in: groupIds}}).toArray()).forEach(i => bulk.requirements.insert(i));
		(await settlin.conversations.find({'group.id': {$in: groupIds}}).toArray()).forEach(i => bulk.conversations.find({_id: i._id}).upsert().replaceOne(i));
		(await settlin.sellerProjects.find({'group.id': {$in: groupIds}}).toArray()).forEach(i => bulk.sellerProjects.insert(i));
		(await settlin.incentives.find({'doc.id': {$in: groupIds}}).toArray()).forEach(i => bulk.incentives.insert(i));
		Logger.debug('Groups:', time());

		// users
		const indIds = Object.keys(indIdsObj);
		await settlin.users.find({internal: true}).toArray().forEach(i => bulk.users.insert(i));
		await settlin.users.find({_id: {$in: indIds}}).toArray().forEach(i => bulk.users.find({_id: i._id}).upsert().replaceOne(i));
		Logger.debug('Users:', time());

		const projectIds = Object.keys(projectIdsObj);
		(await settlin.projects.find({_id: {$in: projectIds}}).toArray()).forEach(i => bulk.projects.insert(i));
		(await settlin.assignees.find({'doc.collection': 'projects', 'doc.id': {$in: projectIds}}).toArray()).forEach(x => bulk.assignees.insert(x));
		(await settlin.incentives.find({'doc.id': {$in: projectIds}}).toArray()).forEach(i => bulk.incentives.insert(i));
		Logger.debug('Projects:', time());

		// config
		const configIds = Object.keys(configIdsObj);
		(await settlin.configurations.find({_id: {$in: configIds}}).toArray()).forEach(i => bulk.configurations.insert(i));
		Logger.debug('Configurations:', time());

		await Promise.all(collections.map(async c => {
			try {
				bulk[c.name].s?.currentBatch?.operations?.length > 0 && await bulk[c.name].execute();
			}
			catch (e) {
				// ignore
			}
		}));
		Logger.debug('Done:', time());
		return {timeTaken: time()};
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}

exports.handler = async function(event, context, callback) {
	console.log('Reading options from event:\n', event);
	
	await run({date: event.date || new Date(), seconds: event.seconds || 3 * 86400});
}
