/**
 * Backend interfaces for the Sayiir flow builder.
 *
 * These abstract over the concrete builder implementation (NAPI-RS, WASM, etc.)
 * so the pure-TypeScript DSL can be used without native dependencies.
 */

export interface RetryPolicyConfig {
  maxRetries: number;
  initialDelaySecs: number;
  backoffMultiplier: number;
  maxDelaySecs?: number;
}

export interface TaskMetadata {
  displayName?: string;
  description?: string;
  timeoutSecs?: number;
  retries?: RetryPolicyConfig;
  tags?: string[];
  version?: string;
  priority?: number;
}

export interface BranchTask {
  taskId: string;
  metadata?: TaskMetadata;
}

export interface BranchEntry {
  key: string;
  tasks: BranchTask[];
}

export interface CompiledWorkflow {
  workflowId: string;
  definitionHash: string;
  metadataJson?: string;
}

export interface FlowBuilderBackend {
  nextLambdaId(): string;
  then(taskId: string, metadata?: TaskMetadata): void;
  addFork(
    branches: BranchTask[][],
    joinId: string,
    joinMetadata?: TaskMetadata,
  ): void;
  addBranch(
    branches: BranchEntry[],
    defaultBranch?: BranchTask[],
  ): string;
  waitForSignal(
    signalId: string,
    signalName: string,
    timeoutSecs?: number,
  ): void;
  delay(delayId: string, seconds: number): void;
  addLoop(
    bodyTaskId: string,
    bodyMetadata?: TaskMetadata,
    maxIterations?: number,
    onMax?: string,
  ): string;
  addChildWorkflow(childId: string, childBuilder: FlowBuilderBackend): void;
  setMetadataJson(json: string): void;
  build(): CompiledWorkflow;
}
