export class SessionAuthorityFence {
  private active = false;
  private generation = 0;

  activate(): number {
    this.generation += 1;
    this.active = true;
    return this.generation;
  }

  invalidate(): void {
    this.active = false;
  }

  capture(): number {
    return this.generation;
  }

  runIfCurrent(generation: number, action: () => void): boolean {
    if (!this.active || generation !== this.generation) {
      return false;
    }
    action();
    return true;
  }
}
