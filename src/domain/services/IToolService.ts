// src/domain/services/IToolService.ts

export interface IToolService {
  /**
   * Execute a named tool with the given input content.
   * Returns the result as a plain object.
   */
  execute(toolName: string, toolInput: unknown): Promise<Record<string, unknown>>;

  /**
   * Returns whether a given tool name is registered.
   */
  supports(toolName: string): boolean;
}
