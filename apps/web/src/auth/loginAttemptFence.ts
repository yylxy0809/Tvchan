export class LoginAttemptFence {
  private active = true;
  private generation = 0;

  begin(): number {
    this.generation += 1;
    return this.generation;
  }

  isCurrent(attempt: number): boolean {
    return this.active && attempt === this.generation;
  }

  dispose(): void {
    this.active = false;
    this.generation += 1;
  }
}
