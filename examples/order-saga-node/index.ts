/**
 * Order Saga — crash recovery with durable checkpoints.
 *
 * Demonstrates what happens when a multi-step order pipeline fails mid-way
 * and how sayiir automatically resumes from the last checkpoint.
 *
 * Run:
 *   pnpm start
 *
 * What you'll see:
 *   1. First run — shipping fails after payment + inventory succeed
 *   2. "Fix" the shipping service, then resume — skips completed steps
 */

import {
  task,
  flow,
  branch,
  runDurableWorkflow,
  resumeWorkflow,
  InMemoryBackend,
  // For production, swap to:
  // PostgresBackend,
} from "sayiir";

// ---------------------------------------------------------------------------
// Simulated failure toggle
// ---------------------------------------------------------------------------

let shippingDown = true;

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

const validateOrder = task("validate-order", (order: {
  id: string;
  item: string;
  amount: number;
  email: string;
}) => {
  console.log(`  [validate-order] Order ${order.id} is valid`);
  return { ...order, validated: true as const };
});

const chargePayment = task(
  "charge-payment",
  (order: { id: string; amount: number }) => {
    console.log(`  [charge-payment] Charged $${order.amount} → pay_${order.id}`);
    return { paymentId: `pay_${order.id}`, amount: order.amount };
  },
  { timeout: "30s", retry: { maxAttempts: 3, initialDelay: "1s", backoffMultiplier: 2.0 } },
);

const reserveInventory = task("reserve-inventory", (order: { id: string; item: string }) => {
  console.log(`  [reserve-inventory] Reserved 1× ${order.item}`);
  return { item: order.item, reserved: true as const };
});

const arrangeShipping = task(
  "arrange-shipping",
  ([payment, inventory]: [
    { paymentId: string; amount: number },
    { item: string; reserved: boolean },
  ]) => {
    if (shippingDown) {
      throw new Error("Shipping API is down!");
    }
    const trackingId = "TRACK-" + Math.random().toString(36).slice(2, 8).toUpperCase();
    console.log(`  [arrange-shipping] Shipping arranged → ${trackingId}`);
    return { paymentId: payment.paymentId, item: inventory.item, trackingId };
  },
);

const sendConfirmation = task(
  "send-confirmation",
  (result: { paymentId: string; item: string; trackingId: string }) => {
    console.log(`  [send-confirmation] Email sent — tracking ${result.trackingId}`);
    return `Order complete! ${result.item} ships via ${result.trackingId}`;
  },
);

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------

const workflow = flow<{
  id: string;
  item: string;
  amount: number;
  email: string;
}>("order-saga")
  .then(validateOrder)
  .fork([
    branch("payment", chargePayment),
    branch("inventory", reserveInventory),
  ])
  .join("merge", arrangeShipping)
  .then(sendConfirmation)
  .build();

// ---------------------------------------------------------------------------
// Backend — swap to PostgresBackend.connect(url) for production
// ---------------------------------------------------------------------------

const backend = new InMemoryBackend();

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

const order = {
  id: "ORD-42",
  item: "Wireless Keyboard",
  amount: 79.99,
  email: "alice@example.com",
};

const instanceId = `saga-${order.id}`;

// ── First run: shipping is down ──
console.log("=== Run 1: Shipping API is down ===\n");

const run1 = runDurableWorkflow(workflow, instanceId, order, backend);

console.log(`\nStatus: ${run1.status}`);
if (run1.status === "failed") {
  console.log(`Error: ${run1.error}`);
}

// ── "Fix" the shipping service ──
console.log("\n--- Shipping API recovered ---\n");
shippingDown = false;

// ── Resume: skips validate, charge, and inventory (already checkpointed) ──
console.log("=== Run 2: Resume from checkpoint ===\n");

const run2 = resumeWorkflow(workflow, instanceId, backend);

console.log(`\nStatus: ${run2.status}`);
if (run2.status === "completed") {
  console.log(`Result: ${run2.output}`);
}
