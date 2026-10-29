---
name: gui-principles
description: >-
  Design and interaction principles for every graphical user interface: web
  pages, dashboards, desktop and terminal UIs, artifacts with a UI, and any
  CSS, layout, pane, table, dialog, or theme work. Load before designing,
  building, refining, or reviewing a GUI, a front-end component, or a page
  layout, including small styling tweaks.
---

# GUI Principles

## Overall character

- Quiet and spacious by default. The default view shows what the task needs
  now and nothing else; everything not strictly necessary is one deliberate
  step away, behind a tab, a section collapsed by default, a pane closed until
  asked for, a menu, or a modal. Free space is a feature, not something to
  fill. Nothing is shown just because it can be counted. A deliberate
  exception (something that would never be discovered if hidden) is named and
  argued, never the default.

- Do not draw anything that hints at content that does not exist yet: no empty
  sections, tours, or placeholder panels on a first run. Empty states say where
  things will appear and offer the one useful next step.

- Choose sensible defaults so that pressing the primary button without
  touching anything does the right thing; prefer a curated to-do list over a
  popup or a question when a decision cannot be defaulted.

- Short lists by default. A list that grows (recent items, history) shows its
  most recent few and lets the user pin what to keep in view; the full list,
  and records of past actions, live in their own tab, pane, or dialog.

- Avoid long vertical scrolling. Design each page to fit the window at its
  default size; long content goes in a bounded container that scrolls by
  itself, or behind tabs. Settings show one section at a time.

- One primary action per page, at the top right of the page's content, where it
  stays in view. Parameters used once per action (destination, file name) are
  asked for in that action's dialog, not shown on the page permanently. A
  page needs no heading that merely repeats what its primary action says.

- A list's detail pane stays closed until the user picks a row, so the list
  has the whole width until then.

- Navigation implies no order unless the work truly has one: pages are peers,
  not numbered steps or a route. Offer the workflow's next step from the page
  the user is on. Keep navigation small, out of the way, and collapsible.

- No emojis, anywhere. Icons are line glyphs drawn from one set. Screen every
  glyph and mark at its real sizes for unintended readings (an eye, a face, a
  fish, a target, a rifle scope), and for collisions with the theme switch's
  sun and moon.

- Hollow before filled: markers, tags, and outlines are drawn hollow; fill is
  reserved for what it measures (empty, half, and full to show progress), and
  selection is shown by a heavier stroke, a ring, a color, or a label.

- Color carries state and nothing else; everything that is not state is a
  neutral. The accent is for the primary action, focus, and selection, never
  decoration. Define every color as a token (components never use a literal
  that works in one theme only), and keep text at 4.5:1 contrast or better.

- Design the light and dark themes separately, each with its own accent and
  palette, rather than inverting one into the other.

- Avoid the look of a generic AI-app or component-library default: no sparkle
  or magic-wand icons, gradients on text or buttons, glows, glass blur, aurora
  backgrounds, purple-to-blue palettes, chat-bubble decoration, or giant
  headline numbers that report nothing actionable. Draw the look from a real
  artifact of the domain (a drawing sheet, a ledger, an instrument panel),
  choose typefaces on purpose instead of the default sans of current web apps,
  and set corners, rules, and density deliberately. Prefer hairline rules to
  floating cards, with shadows only on things that float.

- Use tabular figures and a monospace face for identifiers, counts, dates, and
  keys. State quantities exactly ("24 of 41 done", never "60% done!").

- Animate only to explain a change of place (a pane opening, a row moving to
  its sorted position), and respect `prefers-reduced-motion`.

- On-screen writing: absolute ISO dates (YYYY-MM-DD) with 24-hour times; spell
  out acronyms unless the audience reads them daily; avoid "magic", "smart",
  "AI-powered", "copilot", and "Let's". An error shows a plain message and a
  stable code first, with a show-details expander for the cause, and says what
  went wrong and how to fix it, without apology.

## Interaction and layout

- Editable fields must look editable before hover or focus. Give them a
  persistent label, a visibly bounded textbox with a background or border that
  stands apart from its surroundings, internal padding, and an I-beam cursor.
  Hover and focus styling add emphasis; they must not be the only clues that
  text can be edited. Distinguish editable, focused, read-only, and disabled
  states while keeping the contents readable. Information the user needs to
  read must have clear contrast and a distinct visual location instead of
  blending into the surrounding background.

- Clicking outside an active textbox, including empty space inside a scroll
  container, ends editing and restores its idle or view styling. Preserve the
  field's existing draft and save policy; leaving the field must not submit the
  surrounding form. Clicking inside the field or its scrollbars keeps editing
  active. Do not rely on another control taking focus to dismiss an editor.

- Always use modal dialogs for adding or editing content, not expanding inline
  forms that shift page content or auto-scroll the page.

- Preserve navigation context and make returning to the previous view easy;
  avoid unnecessary navigation away from the user's current content.

- Order pages and controls consistently with their logical workflow or sequence.

- Use tabs, panes, modals, inline expansions, tooltips, and other standard UI
  elements judiciously to organize content and reduce clutter.

- Add tooltips generously: every icon-only control, every badge, chip or
  abbreviation, and every column heading whose meaning is not self-evident
  gets a hover title saying what it is or does. They cost nothing to the
  fluent user and are what lets a first-time or rusty returning user get
  going without a manual.

- Support a light/dark mode switch by default: a circular button at the top
  right of the GUI showing a crescent moon or a sun, with a tooltip. The
  icon shows the side-effect of pressing the button - the mode it switches
  TO - not the mode the page is presently in (a sun while the page is dark,
  a moon while it is light). Follow the system preference until the user
  chooses, persist the choice, and apply it before the first paint so the
  page never flashes the other mode.

- Consider segmented progress bars with discrete stage labels and a continuous,
  draggable tracker for workflows or progress states where appropriate.

- Make panes collapsible with an obvious way to reopen them, so users can
  prioritize their working space.

- Use directional arrows at a pane's top-right edge for collapse/expand controls;
  keep the reopening control visible when the pane is collapsed (a floating
  button is a fine way to do this - see the overlap-check principle below for
  what that needs). Prefer this to the classic stacked horizontal bar triplet.

- Keep long sections in bounded, independently scrollable containers so page
  headings and controls remain visible.

- Text fills the width of its container by default. Do not cap paragraphs,
  lists, or cards at a reading measure (a `max-width` in `ch`) unless asked;
  when a pane is collapsed or the window widens, prose and tables alike take
  the space that opens up. A narrower measure is a request, not a default.

- When a collapsible pane is hidden, re-derive the layout that remains rather
  than letting it fall through. In a CSS grid, an item that becomes
  `display: none` or `position: fixed` stops occupying its track, so its
  siblings auto-place into the first track (often the zero-width one) and the
  main area collapses to a sliver. Switch the template to the tracks that
  remain, or place items explicitly, and measure the main area's width in both
  states.

- Never call `scrollIntoView` (or anything else that can scroll the window) to
  keep a highlighted item visible inside a sticky or fixed pane, such as a
  navigation list that tracks the reader's position. The browser satisfies it
  by scrolling the window toward the item's static position, so the page snaps
  back toward the top on every scroll. Set the pane's own `scrollTop` instead,
  only when the pane overflows, and put `overflow-anchor: none` on sticky
  chrome and inner scroll containers so scroll anchoring cannot latch onto
  them.

- Before handing over a fresh GUI, run a scripted sweep (Playwright or
  similar), not just a screenshot: wheel through the whole page asserting the
  scroll position never moves backward, and toggle every collapsible pane
  asserting the main area keeps its full width and nothing scrolls sideways.

- Floating UI elements (fixed/absolute/sticky positioning, an overlay, a
  floating button or badge, anything not simply laid out in normal document
  flow) are a fine, often-preferable choice for certain use cases. Whenever you
  add one, check that its default position doesn't overlap or cover other UI;
  when it would, adjust its placement (or reserve space for it).

- A control that acts on a scrollable container (a pane's collapse arrow, a
  section's toolbar) must stay visible while that container scrolls. Pin it to
  the scrolling element it belongs to, not merely to the page - a control that
  is sticky at page level still scrolls away inside its own pane's overflow.

- A page's top bar - the strip carrying its title, its global status or
  progress, and its primary actions - stays pinned while the content beneath
  it scrolls. Make every top bar sticky at the top of whatever scrolls under
  it as a matter of course, not as a later enhancement; an unpinned top bar
  is a defect.

- Use color to show state, and cover the intermediate states too, not just the
  binary ones: complete, partially complete, not started, and error each get
  their own color. Keep one palette for one meaning across the whole app - the
  same state in a side pane, a table column, and a badge should look the same -
  and reserve the error color for genuine errors, so a not-yet-started item
  reads as neutral rather than as something gone wrong.

- Size table columns for the content they actually carry rather than leaving it
  to the browser's automatic layout; give text-heavy columns (timestamps,
  descriptions, composite status) enough width that their content stops
  wrapping several lines deep.

- A data table's heading row is always sticky: it stays pinned at the top
  while the rows scroll under it. Note the trap - `position: sticky` sticks
  to the nearest scrolling ancestor, so a table wrapped in an
  `overflow-x: auto` container for sideways scrolling gets no sticky heading
  unless that same container is also what scrolls vertically (bound its
  height and let the table scroll inside it).

- Make data tables sortable by clicking a column heading, toggling ascending
  and descending. Sorts stack hierarchically: clicking a second column refines
  the first as a tie-breaker rather than replacing it, each participating
  heading shows its rank and direction, and an explicit reset control clears
  every key and restores the original order.

    - Where a column's values fall into a handful of categories, give the
      heading a filter too, as a small separate control at the heading's
      right - a funnel-icon button, muted until a filter is set, then
      filled with the accent and showing how many values are selected - so
      it never collides with the heading's own click-to-sort. It opens a
      drop-down of the column's values; an ephemeral reset clears every
      filter, and a note says how many rows are showing while one is active.

- Multi-select lists and drop-downs (a filter's values, a picker of items)
  follow the desktop convention rather than checkboxes: a plain click selects
  that item alone, ctrl-click (cmd on macOS) adds or removes one item, and
  shift-click selects the range from the last plain-clicked item. Show the
  selection by highlighting the item's row, and give the keyboard the same
  three gestures on Enter/Space.

- Let the user resize what holds their content: side panes drag from their
  edge, table columns drag from their heading divider. Give each a visible
  grab strip with a col-resize cursor, clamp it to a sensible min and max,
  remember the chosen size, and offer a way back to the default. A drag strip
  belongs between the things it separates, not overlaid on either of them, and
  a resized table column should grow the table - letting the table scroll
  sideways in its own container - rather than squeezing its neighbors.

    - Honor the conventions users already carry over from other tools. On a
      table column divider, double-click means autofit-to-contents (Excel's
      behavior), so it is the wrong gesture to bind to "restore defaults" -
      give reset its own control instead: an ephemeral one, present only while
      there is something to undo, such as a button above the table's top-left
      corner. A bare circular arrow is ambiguous wherever more than one thing
      can be reset, so pair it with a glyph of what it resets - a `|<-->|`
      width span beside the arrow for column widths, for instance.

    - Ensure that the resized element persists if it is brought back after being
      hidden away, or the user has navigated to another page and returns back.
      This takes away tedious repetitive resizing effort from the user.

- A control that is only operable by dragging needs a keyboard path to the
  same outcome (arrow keys on a focusable handle, or an equivalent control).

- Confirmations and prompts are the page's own modal dialogs, saying exactly
  what will happen and offering Cancel and the named action. Never
  `window.confirm`, `alert`, or `prompt`: hosts such as artifact viewers
  block them.

- Restore the user's place on return: tab, selection, scroll, filters, and
  pane sizes. Remember per-user conveniences (a collapsed bar, a pane width, a
  chosen tab) and restore them after the element was hidden or the user
  navigated away and back.

- Pinned bars stay pinned wherever the page is shown, including inside a
  host's frame. Size a one-window app with `height: 100%` on `html` and
  `body`, not `100vh`; give a top bar `top: env(safe-area-inset-top, 0px)` and
  a bottom bar `padding-bottom: env(safe-area-inset-bottom, 0px)`. Check both
  in a plain window and in a fixed-size frame.

- A table that is a page's subject fills its container down to a small gap
  above whatever is pinned below it, never a fixed height that leaves space
  beneath. One text-heavy column takes the width the others leave, and a
  width the user dragged still wins.

- Known traps: keep CSS-facing names (data attributes,
  custom properties) plain identifiers, separate from storage keys, so a
  key prefix cannot orphan them; a fixed strip at the window's bottom is
  unreachable under a modal dialog, so move it into the dialog or give it a
  keyboard path; a frame that reports its own height to its parent, with a
  resize observer that adds to what it measures, grows forever, so measure the
  content instead.

