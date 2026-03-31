import sys
import os
from pathlib import Path

# Ensure project root is on sys.path when running this script from `scripts/`
HERE = Path(__file__).resolve().parent
ROOT = str(HERE.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from react import actions

ReactionAction = actions.ReactionAction

print("Registered action types:", sorted(ReactionAction.registry.keys()))
ok = True
for name in sorted(ReactionAction.registry.keys()):
    cls = ReactionAction.registry[name]
    desc = getattr(cls, 'description', None)
    if not desc:
        doc = (cls.__doc__ or "").strip().splitlines()
        desc = doc[0].strip() if doc else "(no description)"
    opts = getattr(cls, 'options', [])
    sample = getattr(cls, 'sample', None)
    print(f"- {name}: desc={desc!r}, options={opts}, sample={sample!r}")
    try:
        inst = ReactionAction.create(name, {'roles': []})
        print(f"  instantiate OK: cfg={inst.cfg}")
    except Exception as e:
        ok = False
        print(f"  instantiate FAILED: {e!r}")

# parse_bool tests
print("parse_bool tests:")
vals = [None, True, False, "true", "false", "1", "0", "yes", "no", "on", "off", "unexpected", 2]
for v in vals:
    print(f"  {v!r} -> {ReactionAction._parse_bool(v, default=False)}")

if not ok:
    print("SMOKE FAILED")
    sys.exit(2)
else:
    print("SMOKE OK")
    sys.exit(0)
