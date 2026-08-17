# PR media

Screenshots referenced from pull request descriptions against `warpdotdev/warp`.

Every image is captured through Warp's own integration-test frame capture
(`TestStep::with_take_screenshot`), which reads the app's Metal texture rather than the screen, so
nothing outside Warp's window can appear in a frame. Both halves of every before/after pair come
from the same capture scenario built on the same base commit; the only difference between them is
the branch under review.

This branch carries no code and is never merged.
