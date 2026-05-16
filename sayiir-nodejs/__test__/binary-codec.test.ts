import { describe, it, expect } from "vitest";
import { task, flow, runWorkflow } from "../src/index.js";

/**
 * Binary types (`Buffer`, `Uint8Array`, `ArrayBuffer`) must round-trip
 * through the JS↔Rust codec — i.e. survive being checkpointed as bytes
 * and then handed back to the next task. Naive `serde_json::Value`
 * encoding would mangle them; the codec uses a tagged envelope.
 */
describe("binary value round-trip through the codec", () => {
  it("preserves a Buffer passed between tasks", async () => {
    const produce = task(
      "produce",
      (_n: number): Buffer => Buffer.from([1, 2, 3, 4, 5]),
    );
    const inspect = task("inspect", (buf: Buffer) => ({
      isBuffer: Buffer.isBuffer(buf),
      length: buf.length,
      first: buf[0],
      last: buf[buf.length - 1],
    }));

    const wf = flow<number>("buffer-round-trip")
      .then(produce)
      .then(inspect)
      .build();

    const result = await runWorkflow(wf, 0);
    expect(result).toEqual({ isBuffer: true, length: 5, first: 1, last: 5 });
  });

  it("preserves a Uint8Array passed between tasks", async () => {
    const produce = task(
      "produce",
      (_n: number): Uint8Array => Uint8Array.from([10, 20, 30]),
    );
    const inspect = task("inspect", (u: Uint8Array) => ({
      isUint8Array: u instanceof Uint8Array,
      isBuffer: Buffer.isBuffer(u),
      bytes: Array.from(u),
    }));

    const wf = flow<number>("uint8array-round-trip")
      .then(produce)
      .then(inspect)
      .build();

    const result = await runWorkflow(wf, 0);
    expect(result).toEqual({
      isUint8Array: true,
      isBuffer: false,
      bytes: [10, 20, 30],
    });
  });

  it("preserves an ArrayBuffer passed between tasks", async () => {
    const produce = task("produce", (_n: number): ArrayBuffer => {
      const u8 = new Uint8Array([100, 200, 50]);
      return u8.buffer as ArrayBuffer;
    });
    const inspect = task("inspect", (buf: ArrayBuffer) => {
      const u8 = new Uint8Array(buf);
      return {
        isArrayBuffer: buf instanceof ArrayBuffer,
        byteLength: buf.byteLength,
        bytes: Array.from(u8),
      };
    });

    const wf = flow<number>("arraybuffer-round-trip")
      .then(produce)
      .then(inspect)
      .build();

    const result = await runWorkflow(wf, 0);
    expect(result).toEqual({
      isArrayBuffer: true,
      byteLength: 3,
      bytes: [100, 200, 50],
    });
  });

  it("preserves a Buffer nested inside an object", async () => {
    interface Doc {
      name: string;
      body: Buffer;
      meta: { tags: string[] };
    }

    const produce = task(
      "produce",
      (_n: number): Doc => ({
        name: "doc.bin",
        body: Buffer.from("hello", "utf8"),
        meta: { tags: ["a", "b"] },
      }),
    );
    const inspect = task("inspect", (doc: Doc) => ({
      name: doc.name,
      bodyText: Buffer.isBuffer(doc.body) ? doc.body.toString("utf8") : null,
      tags: doc.meta.tags,
    }));

    const wf = flow<number>("nested-buffer-round-trip")
      .then(produce)
      .then(inspect)
      .build();

    const result = await runWorkflow(wf, 0);
    expect(result).toEqual({
      name: "doc.bin",
      bodyText: "hello",
      tags: ["a", "b"],
    });
  });

  it("preserves a Buffer inside an array", async () => {
    const produce = task(
      "produce",
      (_n: number): [Buffer, string] => [Buffer.from([7]), "tail"],
    );
    const inspect = task("inspect", (items: [Buffer | unknown, string]) => ({
      firstIsBuffer: Buffer.isBuffer(items[0]),
      firstByte: Buffer.isBuffer(items[0])
        ? (items[0] as Buffer)[0]
        : null,
      second: items[1],
    }));

    const wf = flow<number>("array-buffer-round-trip")
      .then(produce)
      .then(inspect)
      .build();

    const result = await runWorkflow(wf, 0);
    expect(result).toEqual({
      firstIsBuffer: true,
      firstByte: 7,
      second: "tail",
    });
  });
});
