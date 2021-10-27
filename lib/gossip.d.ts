import { Callback, Peer } from './types';
export declare class Gossip {
    private readonly ssb;
    private readonly notify;
    private readonly conn;
    private latestWarning;
    constructor(ssb: any, cfg: any);
    private setupConnectionListeners;
    private onConnectingFailed;
    private onConnected;
    private onDisconnected;
    private idToAddr;
    peers: () => any;
    get: (addr: Peer | string) => any;
    connect: (addr: string | Peer, cb: Callback<any>) => void;
    disconnect: (addr: string | Peer, cb: any) => any;
    changes: () => any;
    add: (addr: string | Peer, source: "local" | "pub" | "manual" | "seed" | "friends" | "dht" | "bt" | "stored") => any;
    remove: (addr: string | Peer) => void;
    ping: () => any;
    reconnect: () => void;
    enable: (type: string) => void;
    disable: (type: string) => void;
}
