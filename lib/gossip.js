"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Gossip = void 0;
const secret_stack_decorators_1 = require("secret-stack-decorators");
const pull = require('pull-stream');
const Notify = require('pull-notify');
const ref = require('ssb-ref');
function isPeerObject(o) {
    return o && 'object' == typeof o;
}
function toBase64(s) {
    if (typeof s === 'string')
        return s.substring(1, s.indexOf('.'));
    else
        return s.toString('base64');
}
function toAddressString(address) {
    var _a;
    if (isPeerObject(address)) {
        if (ref.isAddress(address.address))
            return address.address;
        let protocol = 'net';
        if ((_a = address.host) === null || _a === void 0 ? void 0 : _a.endsWith('.onion'))
            protocol = 'onion';
        return ([protocol, address.host, address.port].join(':') +
            '~' +
            ['shs', toBase64(address.key)].join(':'));
    }
    return address;
}
function parseAddress(address) {
    const legacyParsing = ref.parseAddress(address);
    if (legacyParsing) {
        return legacyParsing;
    }
    else if (ref.isAddress(address)) {
        return { key: ref.getKeyFromAddress(address) };
    }
}
function validateAddr(addr) {
    if (!addr || (typeof addr !== 'object' && typeof addr !== 'string')) {
        throw new Error('address should be an object or string');
    }
    const addressString = typeof addr === 'string' ? addr : toAddressString(addr);
    const parsed = typeof addr === 'object' ? addr : parseAddress(addressString);
    if (!parsed.key)
        throw new Error('address must have ed25519 key');
    if (!ref.isFeed(parsed.key))
        throw new Error('key must be ed25519 public id');
    return [addressString, parsed];
}
function inferSource(address) {
    return address.startsWith('net:') ? 'local' : 'manual';
}
let Gossip = class Gossip {
    constructor(ssb, cfg) {
        var _a;
        this.peers = () => {
            if (this.latestWarning + 10e3 < Date.now()) {
                console.trace('DEPRECATED gossip.peers(), use ssb-conn instead');
                this.latestWarning = Date.now();
            }
            const peers = Array.from(this.conn.db().entries()).map(([address, data]) => {
                return Object.assign(Object.assign({}, data), { address, state: this.conn.hub().getState(address) });
            });
            for (const [address, data] of this.conn.hub().entries()) {
                if (!this.conn.db().has(address)) {
                    const [, parsed] = validateAddr(address);
                    peers.push(Object.assign(Object.assign(Object.assign({}, data), parsed), { address, source: inferSource(address) }));
                }
            }
            return peers;
        };
        this.get = (addr) => {
            this.deprecationWarning('get');
            if (ref.isFeed(addr)) {
                for (let [address, data] of this.conn.db().entries()) {
                    if (data.key === addr) {
                        return Object.assign(Object.assign({}, data), { address });
                    }
                }
                return undefined;
            }
            const [addressString] = validateAddr(addr);
            const peer = this.conn.db().get(addressString);
            if (!peer)
                return undefined;
            else {
                return Object.assign({ address: addressString, state: this.conn.hub().getState(addressString) }, peer);
            }
        };
        this.connect = (addr, cb) => {
            var _a, _b;
            this.deprecationWarning('connect');
            let addressString;
            try {
                const inputAddr = ref.isFeed(addr) ? this.idToAddr(addr) : addr;
                [addressString] = validateAddr(inputAddr);
            }
            catch (err) {
                return cb(err);
            }
            this.add(addressString, 'manual');
            const stagedData = (_a = this.conn.staging().get(addressString)) !== null && _a !== void 0 ? _a : {};
            const dbData = (_b = this.conn.db().get(addressString)) !== null && _b !== void 0 ? _b : {};
            const data = Object.assign(Object.assign({}, dbData), stagedData);
            this.conn.connect(addressString, data, cb);
        };
        this.disconnect = (addr, cb) => {
            this.deprecationWarning('disconnect');
            let addressString;
            try {
                const inputAddr = ref.isFeed(addr) ? this.idToAddr(addr) : addr;
                [addressString] = validateAddr(inputAddr);
            }
            catch (err) {
                return cb(err);
            }
            this.conn.disconnect(addressString, cb);
        };
        this.changes = () => {
            this.deprecationWarning('changes');
            return this.notify.listen();
        };
        this.add = (addr, source) => {
            var _a;
            this.deprecationWarning('add');
            const [addressString, parsed] = validateAddr(addr);
            if (parsed.key === this.ssb.id)
                return;
            if (source === 'local') {
                console.error('gossip.add(p, "local") from ssb-local is deprecated, ' +
                    'this only supports ssb-lan');
                return;
            }
            if (this.conn.db().has(addressString)) {
                return this.conn.db().get(addressString);
            }
            else {
                this.conn.db().set(addressString, {
                    host: parsed.host,
                    port: parsed.port,
                    key: parsed.key,
                    address: addressString,
                    source: source,
                });
                this.notify({
                    type: 'discover',
                    peer: Object.assign(Object.assign({}, parsed), { state: this.conn.hub().getState(addressString), source: source !== null && source !== void 0 ? source : 'manual' }),
                    source: source !== null && source !== void 0 ? source : 'manual',
                });
                return (_a = this.conn.db().get(addressString)) !== null && _a !== void 0 ? _a : parsed;
            }
        };
        this.remove = (addr) => {
            this.deprecationWarning('remove');
            const [addressString] = validateAddr(addr);
            this.conn.hub().disconnect(addressString);
            this.conn.staging().unstage(addressString);
            const peer = this.conn.db().get(addressString);
            if (!peer)
                return;
            this.conn.db().delete(addressString);
            this.notify({ type: 'remove', peer: peer });
        };
        this.ping = () => this.conn.ping();
        this.reconnect = () => {
            this.deprecationWarning('reconnect');
            this.conn.hub().reset();
        };
        this.enable = (type) => {
            console.error('UNSUPPORTED gossip.enable("' + type + '") was ignored');
        };
        this.disable = (type) => {
            console.error('UNSUPPORTED gossip.disable("' + type + '") was ignored');
        };
        this.ssb = ssb;
        this.notify = Notify();
        this.conn = this.ssb.conn;
        this.latestWarning = 0;
        this.setupConnectionListeners();
        if (((_a = cfg.conn) === null || _a === void 0 ? void 0 : _a.autostart) === false) {
        }
        else {
            this.conn.start();
        }
        this.deprecationWarned = {};
    }
    setupConnectionListeners() {
        pull(this.conn.hub().listen(), pull.drain((ev) => {
            if (ev.type === 'connecting-failed')
                this.onConnectingFailed(ev);
            if (ev.type === 'connected')
                this.onConnected(ev);
            if (ev.type === 'disconnected')
                this.onDisconnected(ev);
        }));
    }
    onConnectingFailed(ev) {
        const peer = Object.assign({ state: ev.type, address: ev.address, key: ev.key }, this.conn.db().get(ev.address));
        this.notify({ type: 'connect-failure', peer });
    }
    onConnected(ev) {
        const peer = Object.assign({ state: ev.type, address: ev.address, key: ev.key }, this.conn.db().get(ev.address));
        if (!this.conn.db().has(ev.address))
            peer.source = inferSource(ev.address);
        this.notify({ type: 'connect', peer });
    }
    onDisconnected(ev) {
        const peer = Object.assign({ state: ev.type, address: ev.address, key: ev.key }, this.conn.db().get(ev.address));
        this.notify({ type: 'disconnect', peer });
    }
    idToAddr(id) {
        const addr = this.conn.db().getAddressForId(id);
        if (!addr) {
            throw new Error('no known address for peer:' + id);
        }
        return addr;
    }
    deprecationWarning(method) {
        if (this.deprecationWarned[method])
            return;
        console.error(`DEPRECATED gossip.${method}() was called. Use ssb-conn instead`);
        this.deprecationWarned[method] = true;
    }
};
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "peers", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "get", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('async')
], Gossip.prototype, "connect", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('async')
], Gossip.prototype, "disconnect", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('source')
], Gossip.prototype, "changes", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "add", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "remove", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('duplex', { anonymous: 'allow' })
], Gossip.prototype, "ping", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "reconnect", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "enable", void 0);
__decorate([
    secret_stack_decorators_1.muxrpc('sync')
], Gossip.prototype, "disable", void 0);
Gossip = __decorate([
    secret_stack_decorators_1.plugin('1.0.0')
], Gossip);
exports.Gossip = Gossip;
