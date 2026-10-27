import { useEffect, useState } from "react";
import type { LiveMessage } from "../types";

export type SocketStatus = "connecting" | "open" | "closed";

/** ws:// or wss:// URL of the API socket on the same host as the page (nginx or the Vite proxy). */
export function defaultSocketUrl(): string {
  const scheme = window.location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${window.location.host}/ws`;
}

/**
 * Keeps one WebSocket open and reconnects with exponential backoff (1 s up to 30 s).
 * Returns the connection status and the latest message.
 */
export function useLiveSocket(url: string = defaultSocketUrl()) {
  const [status, setStatus] = useState<SocketStatus>("connecting");
  const [lastMessage, setLastMessage] = useState<LiveMessage | null>(null);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let retryTimer: number | undefined;
    let attempt = 0;
    let stopped = false;

    const connect = () => {
      setStatus("connecting");
      socket = new WebSocket(url);
      socket.onopen = () => {
        attempt = 0;
        setStatus("open");
      };
      socket.onmessage = (event: MessageEvent<string>) => {
        try {
          setLastMessage(JSON.parse(event.data) as LiveMessage);
        } catch {
          // plain-text replies such as "pong" are not JSON
        }
      };
      socket.onclose = () => {
        setStatus("closed");
        if (!stopped) {
          retryTimer = window.setTimeout(connect, Math.min(30_000, 1000 * 2 ** attempt));
          attempt += 1;
        }
      };
    };

    connect();
    return () => {
      stopped = true;
      window.clearTimeout(retryTimer);
      socket?.close();
    };
  }, [url]);

  return { status, lastMessage };
}
