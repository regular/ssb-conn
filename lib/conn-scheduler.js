"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConnScheduler = void 0;
const z = require("ziii");
const secret_stack_decorators_1 = require("secret-stack-decorators");
const ConnQuery = require("ssb-conn-query");
const pull = require('pull-stream');
const Pausable = require('pull-pause');
const ip = require('ip');
const onWakeup = require('on-wakeup');
const onNetwork = require('on-change-network-strict');
const hasNetwork = require('has-network2');
const Ref = require('ssb-ref');
const debug = require('debug')('ssb:conn:scheduler');
let lastCheck = 0;
let lastValue = null;
function hasNetworkDebounced() {
    if (lastCheck + 1e3 < Date.now()) {
        lastCheck = Date.now();
        lastValue = hasNetwork();
    }
    return lastValue;
}
function isLocalhost(p) {
    const isLocal = ip.isLoopback(p[1].host) || p[1].host == 'localhost';
    if (p[0].startsWith('ws')) {
        debug('islocal=%o %O', isLocal, p);
    }
    return isLocal;
}
function isOffline(p) {
    if (isLocalhost(p))
        return false;
    else
        return !hasNetworkDebounced();
}
const canBeConnected = (p) => !isOffline(p);
function isLegacy(peer) {
    return hasSuccessfulAttempts(peer) && !hasPinged(peer);
}
function notRoom(peer) {
    return peer[1].type !== 'room';
}
function notPub(peer) {
    return peer[1].type !== 'pub';
}
function isDefunct(peer) {
    return peer[1].defunct === true;
}
function take(n) {
    return (arr) => arr.slice(0, Math.max(n, 0));
}
function detectType(peer) {
    const [addr, data] = peer;
    if (isLocalhost(peer))
        return 'device';
    if (data.type === 'bt')
        return 'bt';
    if (data.type === 'lan')
        return 'lan';
    if (data.type === 'internet')
        return 'internet';
    if (data.type === 'dht')
        return 'dht';
    if (data.type === 'pub')
        return 'pub';
    if (data.type === 'room')
        return 'room';
    if (data.type === 'room-endpoint')
        return 'room-attendant';
    if (data.type === 'room-attendant')
        return 'room-attendant';
    if (data.source === 'local')
        return 'lan';
    if (data.source === 'pub')
        return 'pub';
    if (data.source === 'internet')
        return 'internet';
    if (data.source === 'dht')
        return 'dht';
    if (data.inferredType === 'bt')
        return 'bt';
    if (data.inferredType === 'lan')
        return 'lan';
    if (data.inferredType === 'dht')
        return 'dht';
    if (data.inferredType === 'internet')
        return 'internet';
    if (addr.startsWith('bt:'))
        return 'bt';
    if (addr.startsWith('dht:'))
        return 'dht';
    return '?';
}
const { passesExpBackoff, passesGroupDebounce, hasNoAttempts, hasOnlyFailedAttempts, hasPinged, hasSuccessfulAttempts, sortByStateChange, } = ConnQuery;
function shufflePeers(peers) {
    return peers.sort(() => Math.random() - 0.5);
}
const MINUTES = 60e3;
const HOUR = 60 * 60e3;
let ConnScheduler = class ConnScheduler {
    constructor(ssb, config) {
        this.weBlockThem = ([_addr, data]) => {
            if (!(data === null || data === void 0 ? void 0 : data.key))
                return false;
            return this.socialGraph[data.key] === -1;
        };
        this.weFollowThem = ([_addr, data]) => {
            if (!(data === null || data === void 0 ? void 0 : data.key))
                return false;
            return this.socialGraph[data.key] > 0;
        };
        this.pin = (address) => {
            debug('pinned address: %s', address);
            this.pinnedAddresses.add(address);
        };
        this.unpin = (address) => {
            this.pinnedAddresses.delete(address);
        };
        this.start = () => {
            var _a, _b, _c;
            if (!this.closed)
                return;
            this.closed = false;
            for (let peer of this.ssb.conn.dbPeers()) {
                const [address, { source, type }] = peer;
                if (source === 'local' ||
                    source === 'bt' ||
                    type === 'lan' ||
                    type === 'bt') {
                    this.ssb.conn.forget(address);
                }
                if (isDefunct(peer)) {
                    this.removeDefunct(address);
                }
            }
            this.ssbDB2Subscription = (_a = this.ssb.db) === null || _a === void 0 ? void 0 : _a.post((msg) => {
                if (msg.value.author !== this.ssb.id) {
                    this.lastMessageAt = Date.now();
                }
            });
            this.loadSocialGraph();
            this.populateWithSeeds();
            this.setupPubDiscovery();
            (_b = this.pubDiscoveryPausable) === null || _b === void 0 ? void 0 : _b.resume();
            this.setupLanDiscovery();
            this.setupBluetoothDiscovery();
            this.intervalForUpdate = setInterval(() => this.updateSoon(), 2e3);
            if ((_c = this.intervalForUpdate) === null || _c === void 0 ? void 0 : _c.unref)
                this.intervalForUpdate.unref();
            onWakeup(() => {
                if (this.closed)
                    return;
                this.ssb.conn.hub().reset();
            });
            onNetwork(() => {
                if (this.closed)
                    return;
                this.ssb.conn.hub().reset();
            });
            pull(this.ssb.conn.hub().listen(), pull.filter((ev) => ev.type === 'disconnected'), pull.drain(() => this.updateSoon(200)));
            this.updateSoon();
        };
        this.stop = () => {
            var _a, _b, _c;
            (_a = this.pubDiscoveryPausable) === null || _a === void 0 ? void 0 : _a.pause();
            (_b = this.ssb.lan) === null || _b === void 0 ? void 0 : _b.stop();
            (_c = this.ssbDB2Subscription) === null || _c === void 0 ? void 0 : _c.call(this);
            if (this.intervalForUpdate) {
                clearInterval(this.intervalForUpdate);
                this.intervalForUpdate = void 0;
            }
            this.ssb.conn.hub().reset();
            this.closed = true;
        };
        this.ssb = ssb;
        this.config = config;
        this.closed = true;
        this.lastMessageAt = 0;
        this.hasScheduledAnUpdate = false;
        this.loadedSocialGraph = false;
        this.socialGraph = {};
        this.pinnedAddresses = new Set();
    }
    loadSocialGraph() {
        var _a, _b;
        if (!((_a = this.ssb.friends) === null || _a === void 0 ? void 0 : _a.graphStream)) {
            debug('Warning: ssb-friends@5 is missing, scheduling is degraded');
            this.loadedSocialGraph = true;
            return;
        }
        pull((_b = this.ssb.friends) === null || _b === void 0 ? void 0 : _b.graphStream({ live: true, old: true }), pull.drain((g) => {
            if (g[this.ssb.id]) {
                const prev = this.socialGraph;
                const updates = g[this.ssb.id];
                this.socialGraph = { ...prev, ...updates };
            }
            this.loadedSocialGraph = true;
        }));
    }
    isCurrentlyDownloading() {
        return this.lastMessageAt && this.lastMessageAt > Date.now() - 500;
    }
    maxWaitToConnect(peer) {
        const type = detectType(peer);
        switch (type) {
            case 'lan':
                return 30e3;
            case 'bt':
                return 60e3;
            case 'dht':
                return 300e3;
            default:
                return 10e3;
        }
    }
    updateTheseConnections(test, opts) {
        const query = this.ssb.conn.query();
        const peersUp = query.peersInConnection().filter(test).filter((p) => {
            !this.pinnedAddresses.has(p[0]);
        });
        const peersDown = query.peersConnectable('db').filter(test);
        const { quota, backoffStep, backoffMax, groupMin } = opts;
        const excess = peersUp.length > quota * 2 ? peersUp.length - quota : 0;
        const freeSlots = Math.max(quota - peersUp.length, 0);
        z(peersUp)
            .z(sortByStateChange)
            .z(take(excess))
            .forEach(([addr]) => {
            const fuzzyPeriod = (120e3 * (0.5 + Math.random())) / excess;
            setTimeout(() => {
                this.ssb.conn.disconnect(addr);
            }, fuzzyPeriod);
        });
        z(peersDown)
            .z((peers) => peers.filter((p) => !this.weBlockThem(p)))
            .z((peers) => peers.filter(canBeConnected))
            .z((peers) => peers.filter(([, data]) => data.autoconnect !== false))
            .z(passesGroupDebounce(groupMin))
            .z((peers) => peers.filter(passesExpBackoff(backoffStep, backoffMax)))
            .z((peers) => Math.random() <= 0.3 ? shufflePeers(peers) : sortByStateChange(peers))
            .z(take(freeSlots))
            .forEach(([addr, data]) => this.ssb.conn.connect(addr, data));
    }
    updateStagingNow() {
        this.ssb.conn
            .query()
            .peersConnectable('db')
            .filter((p) => !this.weBlockThem(p))
            .filter(([, data]) => data.autoconnect === false)
            .forEach(([addr, data]) => this.ssb.conn.stage(addr, data));
        this.ssb.conn
            .query()
            .peersConnectable('staging')
            .filter(this.weBlockThem)
            .forEach(([addr]) => this.ssb.conn.unstage(addr));
        this.ssb.conn
            .query()
            .peersConnectable('staging')
            .filter(([, data]) => data.type === 'lan')
            .filter(([, data]) => data.stagingUpdated + 10e3 < Date.now())
            .forEach(([addr]) => this.ssb.conn.unstage(addr));
        this.ssb.conn
            .query()
            .peersConnectable('staging')
            .filter(([, data]) => data.type === 'bt')
            .filter(([, data]) => data.stagingUpdated + 30e3 < Date.now())
            .forEach(([addr]) => this.ssb.conn.unstage(addr));
    }
    updateHubNow() {
        var _a;
        const conn = this.ssb.conn;
        if ((_a = this.config.seed) !== null && _a !== void 0 ? _a : true) {
            this.updateTheseConnections((p) => p[1].source === 'seed', {
                quota: 3,
                backoffStep: 2e3,
                backoffMax: 10 * MINUTES,
                groupMin: 1e3,
            });
        }
        if (conn.query().peersInConnection().length === 0) {
            this.updateTheseConnections(() => true, {
                quota: 1,
                backoffStep: 1e3,
                backoffMax: 6e3,
                groupMin: 0,
            });
        }
        this.updateTheseConnections((p) => p[1].type === 'room', {
            quota: 5,
            backoffStep: 5e3,
            backoffMax: 5 * MINUTES,
            groupMin: 5e3,
        });
        this.updateTheseConnections((p) => notRoom(p) && hasPinged(p), {
            quota: 2,
            backoffStep: 10e3,
            backoffMax: 10 * MINUTES,
            groupMin: 5e3,
        });
        this.updateTheseConnections((p) => notRoom(p) && hasNoAttempts(p), {
            quota: 2,
            backoffStep: 30e3,
            backoffMax: 30 * MINUTES,
            groupMin: 15e3,
        });
        this.updateTheseConnections((p) => notRoom(p) && hasOnlyFailedAttempts(p), {
            quota: 3,
            backoffStep: 1 * MINUTES,
            backoffMax: 3 * HOUR,
            groupMin: 5 * MINUTES,
        });
        this.updateTheseConnections((p) => notRoom(p) && isLegacy(p), {
            quota: 1,
            backoffStep: 4 * MINUTES,
            backoffMax: 3 * HOUR,
            groupMin: 5 * MINUTES,
        });
        z(conn
            .query()
            .peersConnectable('staging')
            .filter(this.weFollowThem)
            .filter(notPub))
            .z(take(3 -
            conn
                .query()
                .peersInConnection()
                .filter(this.weFollowThem)
                .filter(notPub).length))
            .forEach(([addr, data]) => conn.connect(addr, data));
        conn
            .query()
            .peersInConnection()
            .filter(this.weBlockThem)
            .forEach(([addr]) => conn.disconnect(addr));
        conn
            .query()
            .peersInConnection()
            .filter((p) => conn.hub().getState(p[0]) === 'connecting')
            .filter((p) => p[1].stateChange + this.maxWaitToConnect(p) < Date.now())
            .forEach(([addr]) => conn.disconnect(addr));
        conn
            .query()
            .peersConnected()
            .filter((p) => p[1].type !== 'bt' && p[1].type !== 'lan')
            .filter((p) => p[1].stateChange + 0.5 * HOUR < Date.now())
            .forEach(([addr]) => conn.disconnect(addr));
    }
    updateNow() {
        if (this.closed)
            return;
        if (this.isCurrentlyDownloading())
            return;
        if (!this.loadedSocialGraph)
            return;
        this.updateStagingNow();
        this.updateHubNow();
    }
    updateSoon(period = 1000) {
        if (this.closed)
            return;
        if (this.hasScheduledAnUpdate)
            return;
        const fuzzyPeriod = period * 0.5 + period * Math.random();
        this.hasScheduledAnUpdate = true;
        const timer = setTimeout(() => {
            this.updateNow();
            this.hasScheduledAnUpdate = false;
        }, fuzzyPeriod);
        if (timer.unref)
            timer.unref();
    }
    removeDefunct(addr) {
        this.ssb.conn.db().update(addr, { defunct: void 0, autoconnect: void 0 });
    }
    populateWithSeeds() {
        var _a;
        const seeds = (_a = this.config.seeds) !== null && _a !== void 0 ? _a : [];
        (Array.isArray(seeds) ? seeds : [seeds]).filter(Boolean).forEach((addr) => {
            const key = Ref.getKeyFromAddress(addr);
            this.ssb.conn.remember(addr, { key, source: 'seed' });
        });
    }
    setupPubDiscovery() {
        var _a, _b;
        if (((_a = this.config.conn) === null || _a === void 0 ? void 0 : _a.populatePubs) === false)
            return;
        if (!((_b = this.ssb.db) === null || _b === void 0 ? void 0 : _b.operators)) {
            debug('Warning: ssb-db2 is missing, scheduling is degraded');
            return;
        }
        setTimeout(() => {
            var _a, _b;
            if (this.closed)
                return;
            if (!((_a = this.ssb.db) === null || _a === void 0 ? void 0 : _a.operators))
                return;
            const MAX_STAGED_PUBS = 3;
            const { where, type, live, toPullStream } = this.ssb.db.operators;
            this.pubDiscoveryPausable = (_b = this.pubDiscoveryPausable) !== null && _b !== void 0 ? _b : Pausable();
            pull(this.ssb.db.query(where(type('pub')), live({ old: true }), toPullStream()), pull.filter((msg) => { var _a; return Ref.isAddress((_a = msg.value.content) === null || _a === void 0 ? void 0 : _a.address); }), pull.asyncMap((x, cb) => setTimeout(() => cb(null, x), 250)), this.pubDiscoveryPausable, pull.drain((msg) => {
                try {
                    const address = Ref.toMultiServerAddress(msg.value.content.address);
                    const key = Ref.getKeyFromAddress(address);
                    if (this.weBlockThem([address, { key }])) {
                        this.ssb.conn.forget(address);
                    }
                    else if (!this.ssb.conn.db().has(address)) {
                        this.ssb.conn.stage(address, { key, type: 'pub' });
                        this.ssb.conn.remember(address, {
                            key,
                            type: 'pub',
                            autoconnect: false,
                        });
                    }
                }
                catch (err) {
                    debug('cannot process discovered pub because: %s', err);
                }
            }));
            pull(this.ssb.conn.staging().liveEntries(), pull.drain((staged) => {
                var _a, _b;
                if (this.closed)
                    return;
                const stagedPubs = staged.filter(([, data]) => data.type === 'pub');
                if (stagedPubs.length >= MAX_STAGED_PUBS) {
                    (_a = this.pubDiscoveryPausable) === null || _a === void 0 ? void 0 : _a.pause();
                }
                else {
                    (_b = this.pubDiscoveryPausable) === null || _b === void 0 ? void 0 : _b.resume();
                }
            }));
        }, 1000);
    }
    setupBluetoothDiscovery() {
        var _a;
        if (!((_a = this.ssb.bluetooth) === null || _a === void 0 ? void 0 : _a.nearbyScuttlebuttDevices)) {
            debug('Warning: ssb-bluetooth is missing, scheduling is degraded');
            return;
        }
        pull(this.ssb.bluetooth.nearbyScuttlebuttDevices(1000), pull.drain(({ discovered }) => {
            if (this.closed)
                return;
            for (const btPeer of discovered) {
                const address = `bt:${btPeer.remoteAddress.split(':').join('')}` +
                    '~' +
                    `shs:${btPeer.id.replace(/^\@/, '').replace(/\.ed25519$/, '')}`;
                const data = {
                    type: 'bt',
                    note: btPeer.displayName,
                    key: btPeer.id,
                };
                if (this.weFollowThem([address, data])) {
                    this.ssb.conn.connect(address, data);
                }
                else {
                    this.ssb.conn.stage(address, data);
                }
            }
        }));
    }
    setupLanDiscovery() {
        var _a, _b;
        if (!((_a = this.ssb.lan) === null || _a === void 0 ? void 0 : _a.start) || !((_b = this.ssb.lan) === null || _b === void 0 ? void 0 : _b.discoveredPeers)) {
            debug('Warning: ssb-lan is missing, scheduling is degraded');
            return;
        }
        pull(this.ssb.lan.discoveredPeers(), pull.drain(({ address, verified }) => {
            const key = Ref.getKeyFromAddress(address);
            if (!key)
                return;
            const data = {
                type: 'lan',
                key,
                verified,
            };
            if (this.weFollowThem([address, data])) {
                this.ssb.conn.connect(address, data);
            }
            else {
                this.ssb.conn.stage(address, data);
            }
        }));
        this.ssb.lan.start();
    }
};
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], ConnScheduler.prototype, "pin", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], ConnScheduler.prototype, "unpin", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], ConnScheduler.prototype, "start", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], ConnScheduler.prototype, "stop", void 0);
ConnScheduler = __decorate([
    secret_stack_decorators_1.plugin('1.0.0')
], ConnScheduler);
exports.ConnScheduler = ConnScheduler;
