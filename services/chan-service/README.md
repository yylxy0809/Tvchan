# Chan Service

Formal Chan engine:

- `Vespa314/chan.py`
- configured with `CHAN_ENGINE_MODE=module_b`
- vendor path configured with `CHAN_PY_PATH`

The service adapter only converts project bars to `chan.py` inputs and converts
`chan.py` outputs back to the project contract. Inclusion, fractal, stroke,
segment, center, and buy/sell-point calculations must remain inside `chan.py`.
