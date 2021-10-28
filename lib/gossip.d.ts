import { Callback, Peer } from './types';
export declare class Gossip {
    private readonly ssb;
    private readonly notify;
    private readonly conn;
    private deprecationWarned;
    private latestWarning;
    constructor(ssb: any, cfg: any);
    private setupConnectionListeners;
    private onConnectingFailed;
    private onConnected;
    private onDisconnected;
    private idToAddr;
    private deprecationWarning;
    peers: () => any;
    get: (addr: Peer | string) => any;
    connect: (addr: Peer | string, cb: Callback<any>) => void;
    disconnect: (addr: Peer | string, cb: any) => any;
    changes: () => any;
    add: (addr: Peer | string, source: Peer['source']) => any;
    remove: (addr: Peer | string) => void;
    ping: () => any;
    reconnect: () => void;
    enable: (type: string) => void;
    disable: (type: string) => void;
}
