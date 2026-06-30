import { RotateCcw, SlidersHorizontal } from "lucide-react";
import type { ReactNode } from "react";
import {
  CHAN_LEVELS,
  CHAN_MODES,
  CHAN_PARTS,
  type ChanLevel,
  type ChanMode,
  type ChanOverlayPart,
  type ChanOverlaySettings,
} from "../tradingview/overlaySettings";

type Props = {
  settings: ChanOverlaySettings;
  onOpenSettings(): void;
  onReset(): void;
};

const LEVEL_LABELS: Record<ChanLevel, string> = {
  "5f": "5f",
  "30f": "30f",
  "1d": "日线",
};

const MODE_LABELS: Record<ChanMode, string> = {
  confirmed: "已完成",
  predictive: "构建中",
};

const PART_LABELS: Record<ChanOverlayPart, string> = {
  strokes: "笔",
  segments: "线段",
  centers: "中枢",
  signals: "买卖点",
  channels: "plot_channel",
};
export function ChanOverlayControls({ settings, onOpenSettings, onReset }: Props) {
  return (
    <aside className="chan-controls" aria-label="Chan overlay controls">
      <div className="control-header">
        <div>
          <SlidersHorizontal size={16} />
          <span>Chan Overlay</span>
        </div>
        <div className="control-actions">
          <button
            type="button"
            className="ghost-icon-button"
            title="指标设置"
            onClick={onOpenSettings}
          >
            <SlidersHorizontal size={15} />
          </button>
          <button
            type="button"
            className="ghost-icon-button"
            title="重置叠加层"
            onClick={onReset}
          >
            <RotateCcw size={15} />
          </button>
        </div>
      </div>

      <Readout label="级别">
        {CHAN_LEVELS.map((level) => (
          <ReadoutChip
            key={level}
            active={settings.levels[level]}
            color={settings.styles[level].stroke.color}
            label={LEVEL_LABELS[level]}
          />
        ))}
      </Readout>

      <Readout label="类型">
        {CHAN_PARTS.map((part) => (
          <ReadoutChip
            key={part}
            active={settings.parts[part]}
            label={PART_LABELS[part]}
          />
        ))}
      </Readout>

      <Readout label="状态">
        {CHAN_MODES.map((mode) => (
          <ReadoutChip
            key={mode}
            active={settings.modes[mode]}
            label={`${MODE_LABELS[mode]} ${lineStyleLabel(settings.lineStyles[mode])}`}
          />
        ))}
      </Readout>

      <Readout label="样式">
        {CHAN_LEVELS.map((level) => {
          const style = settings.styles[level];
          return (
            <ReadoutChip
              key={level}
              active={settings.levels[level]}
              color={style.segment.color}
              label={`${LEVEL_LABELS[level]} 笔${style.stroke.linewidth}/段${style.segment.linewidth}/中${style.center.linewidth}/通${style.channel.linewidth}`}
            />
          );
        })}
      </Readout>
    </aside>
  );
}

function lineStyleLabel(value: number): string {
  if (value === 0) {
    return "实线";
  }
  if (value === 1) {
    return "点线";
  }
  return "虚线";
}

function Readout({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="control-group">
      <span>{label}</span>
      <div className="check-grid">{children}</div>
    </div>
  );
}

function ReadoutChip({
  active,
  color,
  label,
}: {
  active: boolean;
  color?: string;
  label: string;
}) {
  return (
    <span className="readout-chip" data-active={active}>
      {color ? <i style={{ backgroundColor: color }} /> : null}
      <span>{label}</span>
    </span>
  );
}

