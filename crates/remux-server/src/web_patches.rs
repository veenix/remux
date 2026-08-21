/// CSS injected into `<head>` of every HTML response.
/// Targets stable `data-*` attributes and semantic class names rather than
/// minified JS internals, so it survives jellyfin-web bundle updates.
pub static CSS: &str = r##"
  /* ── Sidebar: whole sections ─────────────────────────────── */
  [aria-labelledby="plugins-subheader"]   { display: none !important; }

  /* ── Async media-sources spinner ─────────────────────────── */
  @keyframes remux-spin {
    to { transform: rotate(360deg); }
  }

  /* ── Play button: disabled by default, enabled when streams arrive ── */
  .detailPagePrimaryContainer .btnPlay {
    opacity: 0.4 !important;
    pointer-events: none !important;
    cursor: default !important;
  }
  .detailPagePrimaryContainer.remux-streams-ready .btnPlay {
    opacity: 1 !important;
    pointer-events: auto !important;
    cursor: pointer !important;
  }

  /* Keep the existing Back control pointer-accessible until video actually starts. */
  html.remux-playback-starting .skinHeader {
    z-index: 1001;
  }

  /* Keep the header's actual controls available to the pointer even when an
     older webOS engine leaves a non-interactive state on the header shell. */
  html.layout-tv .skinHeader .headerTop,
  html.layout-tv .skinHeader .headerTabs,
  html.layout-tv .skinHeader button,
  html.layout-tv .skinHeader a,
  html.layout-tv .skinHeader [role="button"] {
    pointer-events: auto !important;
  }

  .remux-startup-status {
    position: fixed;
    left: 50%;
    bottom: 12%;
    z-index: 1002;
    box-sizing: border-box;
    width: calc(100% - 4em);
    max-width: 36em;
    padding: 1em 1.25em;
    transform: translateX(-50%);
    border-radius: 0.45em;
    background: rgba(0, 0, 0, 0.74);
    color: #fff;
    text-align: center;
    pointer-events: none;
  }
  .remux-startup-title {
    font-size: 1.2em;
    font-weight: 500;
  }
  .remux-startup-detail,
  .remux-startup-footnote {
    margin-top: 0.45em;
    color: rgba(255, 255, 255, 0.78);
    font-size: 0.86em;
    line-height: 1.35;
  }
  .remux-startup-progress {
    height: 0.3em;
    margin-top: 0.8em;
    overflow: hidden;
    border-radius: 0.2em;
    background: rgba(255, 255, 255, 0.2);
  }
  .remux-startup-progress-bar {
    width: 0;
    height: 100%;
    border-radius: inherit;
    background: #00a4dc;
    transition: width 0.4s linear;
  }
  .remux-startup-progress-bar.remux-indeterminate {
    width: 35%;
    animation: remux-progress-slide 1.3s ease-in-out infinite;
  }
  .remux-startup-status.remux-startup-error {
    bottom: 50%;
    transform: translate(-50%, 50%);
    pointer-events: auto;
  }
  .remux-startup-error .remux-startup-progress {
    display: none;
  }
  .remux-startup-back {
    min-width: 8em;
    margin-top: 1em;
    padding: 0.65em 1.2em;
    border: 0;
    border-radius: 0.3em;
    background: #00a4dc;
    color: #fff;
    font: inherit;
    cursor: pointer;
  }
  @keyframes remux-progress-slide {
    from { transform: translateX(-110%); }
    to { transform: translateX(310%); }
  }
"##;

/// JS injected before `</body>` of every HTML response.
/// Intercepts React Router (History API) navigation to /wizard and /dashboard
/// and redirects to our admin UI at /admin.
pub static JS: &str = r#"

(function () {
  var ADMIN = ['/wizard', '/dashboard'];

  function matchesAdmin(p) {
    for (var i = 0; i < ADMIN.length; i++) {
      var a = ADMIN[i];
      if (p === a || p.startsWith(a + '/') || p.startsWith(a + '?')) return true;
    }
    return false;
  }

  // Check both pathname and hash (createHashRouter stores route in hash)
  function checkUrl(url) {
    try {
      var u = new URL(String(url), location.href);
      if (matchesAdmin(u.pathname)) { location.replace('/admin'); return true; }
      if (u.hash) {
        var h = '/' + u.hash.replace(/^#\/?/, '');
        if (matchesAdmin(h)) { location.replace('/admin'); return true; }
      }
    } catch(e) {}
    return false;
  }

  function checkCurrent() {
    return checkUrl(location.href);
  }

  if (checkCurrent()) return;

  // Intercept React Router History API (covers both BrowserRouter and HashRouter)
  var _push = history.pushState.bind(history);
  var _replace = history.replaceState.bind(history);
  history.pushState = function(s, t, url) {
    if (url && checkUrl(url)) return;
    return _push(s, t, url);
  };
  history.replaceState = function(s, t, url) {
    if (url && checkUrl(url)) return;
    return _replace(s, t, url);
  };
  window.addEventListener('popstate', checkCurrent);
  window.addEventListener('hashchange', checkCurrent);
}());

(function () {
  var _get = Storage.prototype.getItem;
  Storage.prototype.getItem = function (key) {
    var val = _get.call(this, key);
    if (typeof key === 'string' && /maxbitrate-video-false/i.test(key) && (val === null || val === '15000')) {
      return '0';
    }
    return val;
  };
}());

// Async MediaSources loader for the item details page.
// Patches ApiClient.prototype.getItem (available via window.ApiClient) to skip
// stream loading on the initial fetch (Fields=ChildCount), making the server
// respond faster. For Movie/Episode a spinner appears while a second getItem call
// retrieves MediaSources and populates the track-selection UI.
(function () {
  var _videoNavCount = 0; // increments each time a video item page is entered

  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function getDetailsPage() {
    // Jellyfin caches views: multiple detail-page trees may exist in the DOM
    // (one per cached view). Anchor on the always-visible primary container and
    // use offsetParent to find the visible view.
    var all = document.querySelectorAll('.detailPagePrimaryContainer');
    for (var i = 0; i < all.length; i++) {
      if (all[i].offsetParent !== null) return all[i].querySelector('.detailPagePrimaryContent');
    }
    return null;
  }

  function getVisiblePrimaryContainer() {
    var all = document.querySelectorAll('.detailPagePrimaryContainer');
    for (var i = 0; i < all.length; i++) {
      if (all[i].offsetParent !== null) return all[i];
    }
    return null;
  }

  function hideTrackControls(page) {
    var form = page.querySelector('.trackSelections');
    if (!form) return;
    var containers = form.querySelectorAll('.selectSourceContainer, .selectVideoContainer, .selectAudioContainer, .selectSubtitlesContainer');
    for (var i = 0; i < containers.length; i++) containers[i].classList.add('hide');
  }

  function showSpinner(page) {
    removeSpinner(page);
    var form = page.querySelector('.trackSelections');
    if (!form) return;
    // Hide the stub-rendered selects but keep the outer panel: the spinner
    // renders inside it, centered like the track fields do after load.
    hideTrackControls(page);
    var spin = document.createElement('div');
    spin.className = 'remux-sources-loading';
    // margin:auto centres the item in any flex or block context the theme uses
    spin.style.cssText = 'width:1.4em;height:1.4em;border:2px solid rgba(255,255,255,0.2);border-top-color:rgba(255,255,255,0.8);border-radius:50%;animation:remux-spin 0.7s linear infinite;margin:0.4em auto;display:block;flex-shrink:0;';
    form.insertBefore(spin, form.firstChild);
    form.classList.remove('hide');
  }

  function removeSpinner(page) {
    var el = page.querySelector('.remux-sources-loading');
    if (el && el.parentNode) el.parentNode.removeChild(el);
    var noStreams = page.querySelector('.remux-no-streams');
    if (noStreams && noStreams.parentNode) noStreams.parentNode.removeChild(noStreams);
    // re-hide the form if sources haven't arrived yet
    var form = page.querySelector('.trackSelections');
    if (form && !form._remuxLoaded) form.classList.add('hide');
  }

  function showNoStreams(page) {
    removeSpinner(page);
    var form = page.querySelector('.trackSelections');
    if (!form) return;
    hideTrackControls(page);
    var msg = document.createElement('div');
    msg.className = 'remux-no-streams';
    msg.style.cssText = 'color:rgba(255,255,255,0.5);font-size:0.85em;text-align:center;padding:0.4em 0;';
    msg.textContent = 'No streams available';
    form.insertBefore(msg, form.firstChild);
    form.classList.remove('hide');
  }

  function disablePlayButton(page) {
    var container = page && page.closest('.detailPagePrimaryContainer');
    if (container) container.classList.remove('remux-streams-ready');
  }

  function enablePlayButton(page) {
    var container = page && page.closest('.detailPagePrimaryContainer');
    if (container) container.classList.add('remux-streams-ready');
  }

  function renderTracksForSource(page, mediaSources, selectedSourceId) {
    // Same guard as renderAsyncTrackSelections: the version-change handler
    // re-renders the track selects, and the observer must not loop on that.
    var form = page.querySelector('.trackSelections');
    if (form) form._remuxRendering = true;
    var source = null;
    for (var i = 0; i < mediaSources.length; i++) {
      if (mediaSources[i].Id === selectedSourceId) { source = mediaSources[i]; break; }
    }
    if (!source) source = mediaSources[0];
    var streams = source.MediaStreams || [];

    // Video — display-only, always disabled
    var videoTracks = streams.filter(function (s) { return s.Type === 'Video'; });
    var selVideo = page.querySelector('.selectVideo');
    if (selVideo.setLabel) selVideo.setLabel('Video');
    selVideo.innerHTML = videoTracks.map(function (v) {
      return '<option value="' + v.Index + '" selected>' + escHtml(v.DisplayTitle || v.Codec || String(v.Index)) + '</option>';
    }).join('');
    selVideo.setAttribute('disabled', 'disabled');
    page.querySelector('.selectVideoContainer').classList[videoTracks.length ? 'remove' : 'add']('hide');

    // Audio
    var audioTracks = streams.filter(function (s) { return s.Type === 'Audio'; });
    var selAudio = page.querySelector('.selectAudio');
    if (selAudio.setLabel) selAudio.setLabel('Audio');
    var defAudio = source.DefaultAudioStreamIndex;
    selAudio.innerHTML = audioTracks.map(function (v) {
      var sel = v.Index === defAudio ? ' selected' : '';
      return '<option value="' + v.Index + '"' + sel + '>' + escHtml(v.DisplayTitle || String(v.Index)) + '</option>';
    }).join('');
    selAudio[audioTracks.length > 1 ? 'removeAttribute' : 'setAttribute']('disabled', 'disabled');
    page.querySelector('.selectAudioContainer').classList[audioTracks.length ? 'remove' : 'add']('hide');

    // Subtitles
    var subTracks = streams.filter(function (s) { return s.Type === 'Subtitle'; });
    var selSubs = page.querySelector('.selectSubtitles');
    if (selSubs.setLabel) selSubs.setLabel('Subtitles');
    var defSub = source.DefaultSubtitleStreamIndex == null ? -1 : source.DefaultSubtitleStreamIndex;
    var offSel = defSub === -1 ? ' selected' : '';
    selSubs.innerHTML = '<option value="-1"' + offSel + '>Off</option>' + subTracks.map(function (v) {
      var sel = v.Index === defSub ? ' selected' : '';
      return '<option value="' + v.Index + '"' + sel + '>' + escHtml(v.DisplayTitle || String(v.Index)) + '</option>';
    }).join('');
    selSubs[subTracks.length ? 'removeAttribute' : 'setAttribute']('disabled', 'disabled');
    page.querySelector('.selectSubtitlesContainer').classList[subTracks.length ? 'remove' : 'add']('hide');
    if (form) setTimeout(function () { form._remuxRendering = false; }, 0);
  }

  // The core re-renders the track selects from its cached item (the fast item
  // with stub MediaSources) on player changes and cached-view restores, wiping
  // our real audio/subtitle dropdowns. Re-apply our loaded data whenever the
  // core touches the panel.
  function attachTrackSelectionsGuard(page) {
    var form = page.querySelector('.trackSelections');
    if (!form || form._remuxObsAttached) return;
    form._remuxObsAttached = true;
    var obs = new MutationObserver(function () {
      if (form._remuxRendering) return;
      if (!form._remuxLoaded) return;
      var ms = form._remuxMediaSources;
      if (!ms || !ms.length) return;
      renderAsyncTrackSelections(page, ms);
    });
    obs.observe(form, { childList: true, subtree: true });
  }

  function renderAsyncTrackSelections(page, mediaSources) {
    var form = page.querySelector('.trackSelections');
    if (!form) return;
    // Guard: the MutationObserver below must not react to our own renders.
    // Observer callbacks run as microtasks before the next macrotask, so a
    // setTimeout(0) clear happens after any observer callback queued by this
    // render has already run (and been skipped).
    form._remuxRendering = true;

    var selSrc = page.querySelector('.selectSource');
    var selectedId = mediaSources[0].Id;
    selSrc.innerHTML = mediaSources.map(function (v) {
      var sel = v.Id === selectedId ? ' selected' : '';
      return '<option value="' + escHtml(v.Id) + '"' + sel + '>' + escHtml(v.Name) + '</option>';
    }).join('');
    if (selSrc.setLabel) selSrc.setLabel('Version');
    page.querySelector('.selectSourceContainer').classList[mediaSources.length > 1 ? 'remove' : 'add']('hide');

    renderTracksForSource(page, mediaSources, selectedId);

    form._remuxMediaSources = mediaSources;
    form._remuxLoaded = true;

    // Hide the whole panel when there are no meaningful choices:
    // single version, at most one audio track, and no subtitles.
    var source = mediaSources[0];
    var streams = source.MediaStreams || [];
    var hasChoice = mediaSources.length > 1
      || streams.filter(function (s) { return s.Type === 'Audio'; }).length > 1
      || streams.some(function (s) { return s.Type === 'Subtitle'; });
    if (hasChoice) {
      form.classList.remove('hide');
    } else {
      form.classList.add('hide');
    }

    setTimeout(function () { form._remuxRendering = false; }, 0);
  }

  // Handle source changes before Jellyfin's listener. The initial lightweight
  // item intentionally has no MediaSources, so Jellyfin's cached source array
  // is empty and its listener cannot render these controls.
  function attachSourceChangeHandler(page) {
    var sel = page.querySelector('.selectSource');
    if (sel._remuxHandlerAttached) return;
    sel._remuxHandlerAttached = true;
    sel.addEventListener('change', function (event) {
      var form = page.querySelector('.trackSelections');
      var ms = form && form._remuxMediaSources;
      if (!ms) return;
      event.stopImmediatePropagation();
      renderTracksForSource(page, ms, sel.value);
    }, true);
  }

  function patchApiClientProto(apiClient) {
    var proto = Object.getPrototypeOf(apiClient);
    if (!proto || proto._remuxGetItemPatched) return;
    proto._remuxGetItemPatched = true;
    var _getItem = proto.getItem;

    // Only getItem owns the follow-up MediaSources request below. Generic item
    // fetches must remain intact: reducing one to ChildCount would leave callers
    // that bypass getItem with a permanent stub and no source picker.
    proto.getItem = function (userId, itemId) {
      var self = this;
      // Guard: itemId may be non-string (e.g. undefined) when called from list-view play buttons.
      if (typeof itemId !== 'string') {
        return _getItem.apply(self, arguments);
      }
      var capturedId = itemId;
      // Strip dashes so we can match against both UUID formats in the URL.
      var capturedIdNoDash = itemId.replace(/-/g, '');

      // True when the current URL belongs to this item's detail page.
      // Related-item fetches (next-up cards, season metadata, previews) have IDs
      // that do not appear in the URL, so this returns false for them.
      function isCurrentPage() {
        var h = location.href;
        return h.indexOf(capturedId) !== -1 || h.indexOf(capturedIdNoDash) !== -1;
      }

      // Playback, remote-control, editor, and local-file code also call getItem.
      // Preserve the native request for anything except the item whose details
      // page is currently visible.
      if (!isCurrentPage()) return _getItem.apply(self, arguments);

      var baseUrl = self.getUrl('Users/' + userId + '/Items/' + itemId);
      var sep = baseUrl.indexOf('?') >= 0 ? '&' : '?';
      var fastUrl = baseUrl + sep + 'Fields=ChildCount';

      return self.getJSON(fastUrl).then(function (item) {
        var type = item && item.Type;
        var isMovieOrEpisode = (type === 'Movie' || type === 'Episode');
        // Skip everything for related-item fetches — only process the item whose
        // ID is reflected in the current page URL.
        if (!isCurrentPage()) return item;

        // Shared helper: enable the play button on the VISIBLE primary container.
        // Jellyfin caches old views in the DOM (hidden), so querySelector('.detailPagePrimaryContainer')
        // may return a hidden old view's container. We use offsetParent to find the visible one.
        function watchAndEnable() {
          var deadline = Date.now() + 5000;
          function tryEnable() {
            if (!isCurrentPage()) return;
            var c = getVisiblePrimaryContainer();
            if (c) {
              c.classList.add('remux-streams-ready');
              return;
            }
            if (Date.now() < deadline) setTimeout(tryEnable, 50);
          }
          tryEnable();
        }

        if (!isMovieOrEpisode) {
          // Non-video (Series, Season, etc.): enable play button immediately.
          watchAndEnable();
        } else {
          // Video (Movie/Episode): load streams, then enable play button.
          var capturedNav = ++_videoNavCount;
          var sourcesUrl = baseUrl + sep + 'Fields=MediaSources';
          var sourcesFetch = self.getJSON(sourcesUrl);

          setTimeout(function () {
            if (!isCurrentPage()) return;
            var page = getDetailsPage();
            if (!page) return;
            var form = page.querySelector('.trackSelections');
            if (form && form._remuxNavCount === capturedNav) return;
            showSpinner(page);
          }, 0);

          sourcesFetch.then(function (full) {
            if (!isCurrentPage()) return;
            var ms = full && full.MediaSources;
            var streamsReady = ms && ms.length && full.LocationType !== 'Virtual';

            if (streamsReady) {
              // Jellyfin retains the object returned by the initial request as
              // currentItem. Keep it complete for playback and other native
              // consumers instead of updating only our rendered controls.
              item.MediaSources = ms;
              if (full.MediaStreams) item.MediaStreams = full.MediaStreams;
              // Enable the play button as soon as streams are confirmed.
              watchAndEnable();
            }

            // Best-effort: render the audio/subtitle track selector UI.
            (function apply() {
              if (!isCurrentPage()) return;
              var page = getDetailsPage();
              if (!page) { setTimeout(apply, 50); return; }
              var form = page.querySelector('.trackSelections');
              if (form && form._remuxNavCount === capturedNav) return;
              removeSpinner(page);
              if (streamsReady) {
                renderAsyncTrackSelections(page, ms);
                attachSourceChangeHandler(page);
                attachTrackSelectionsGuard(page);
                var f = page.querySelector('.trackSelections');
                if (f) f._remuxNavCount = capturedNav;
              } else {
                showNoStreams(page);
              }
            }());
          }).catch(function () {
            if (!isCurrentPage()) return;
            var page = getDetailsPage();
            if (page) {
              var form = page.querySelector('.trackSelections');
              if (!form || !form._remuxLoaded) removeSpinner(page);
            }
          });
        }

        return item;
      });
    };
  }

  // Intercept the exact moment window.ApiClient is assigned by ServerConnections.
  // This runs synchronously before any defer scripts, so the trap is in place
  // before the app initialises. No polling needed.
  var _realApiClient = null;
  try {
    Object.defineProperty(window, 'ApiClient', {
      configurable: true,
      get: function () { return _realApiClient; },
      set: function (v) {
        _realApiClient = v;
        if (v) patchApiClientProto(v);
      }
    });
  } catch (e) {
    // Fallback if defineProperty fails (property already sealed): poll instead.
    (function poll() {
      if (window.ApiClient) { patchApiClientProto(window.ApiClient); }
      else { setTimeout(poll, 50); }
    }());
  }

}());

// Jellyfin deliberately disables its global focus-follows-pointer behavior on
// webOS. Keep the Magic Remote pointer and keyboard focus aligned only for the
// shared header. Focusing a TV button scales it; some webOS versions then lose
// the native click between press and release because the target geometry moved.
// Capture that gesture and dispatch exactly one click to the pressed control.
(function () {
  if (!/(web0s|netcast)/i.test(navigator.userAgent || '')) return;

  var pressedHeaderControl = null;
  var pressedAt = null;
  var dispatchingHeaderClick = false;
  var lastActivatedHeaderControl = null;
  var suppressClickUntil = 0;
  var lastPointer = null;

  function hasOpenDialog() {
    var dialogs = document.querySelectorAll('.dialog');
    for (var i = 0; i < dialogs.length; i++) {
      if (!dialogs[i].classList.contains('hide')) return true;
    }
    return false;
  }

  function headerControlFrom(target) {
    var header = document.querySelector('.skinHeader');
    if (!header || !target || !header.contains(target)) return null;

    while (target && target !== header) {
      var tagName = target.tagName;
      if ((tagName === 'BUTTON'
          || tagName === 'A'
          || target.getAttribute('role') === 'button')
          && !target.disabled) {
        return target;
      }
      target = target.parentElement;
    }
    return null;
  }

  function eventPoint(event) {
    if (typeof event.clientX !== 'number' || typeof event.clientY !== 'number') return null;
    return { x: event.clientX, y: event.clientY, at: Date.now() };
  }

  function headerControlAtPoint(point) {
    var header = document.querySelector('.skinHeader');
    if (!header || !point) return null;
    var controls = header.querySelectorAll('button, a, [role="button"]');
    for (var i = 0; i < controls.length; i++) {
      var control = controls[i];
      if (control.disabled) continue;
      var rect = control.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0
          && point.x >= rect.left && point.x <= rect.right
          && point.y >= rect.top && point.y <= rect.bottom) {
        return control;
      }
    }
    return null;
  }

  function headerControlForEvent(event) {
    // Modal dialogs (including audio/subtitle action sheets) own the gesture.
    // Never route their pointer events through to a header control behind them.
    if (hasOpenDialog()) return null;
    var direct = headerControlFrom(event.target);
    if (direct) return direct;
    var point = eventPoint(event);
    return headerControlAtPoint(point || lastPointer);
  }

  function focusControl(control) {
    if (!control || document.activeElement === control) return;
    try {
      control.focus({ preventScroll: true });
    } catch (error) {
      control.focus();
    }
  }

  function focusHeaderControl(event) {
    var point = eventPoint(event);
    if (point) lastPointer = point;
    focusControl(headerControlForEvent(event));
  }

  function activateHeaderControl(control, event) {
    if (!control || !document.documentElement.contains(control)) return;
    if (event) {
      event.preventDefault();
      event.stopImmediatePropagation();
    }
    lastActivatedHeaderControl = control;
    suppressClickUntil = Date.now() + 500;
    dispatchingHeaderClick = true;
    try {
      control.click();
    } finally {
      dispatchingHeaderClick = false;
    }
  }

  function pressHeaderControl(event) {
    var point = eventPoint(event);
    if (point) lastPointer = point;
    var control = headerControlForEvent(event);
    if (!control || (typeof event.button === 'number' && event.button !== 0)) {
      pressedHeaderControl = null;
      pressedAt = null;
      return;
    }

    pressedHeaderControl = control;
    pressedAt = point;
    focusControl(control);
    event.preventDefault();
    event.stopImmediatePropagation();
  }

  function releaseHeaderControl(event) {
    var control = pressedHeaderControl;
    var start = pressedAt;
    pressedHeaderControl = null;
    pressedAt = null;
    if (hasOpenDialog()) return;
    if (!control || !document.documentElement.contains(control)) return;
    if (start && typeof event.clientX === 'number'
        && (Math.abs(event.clientX - start.x) > 24
            || Math.abs(event.clientY - start.y) > 24)) return;

    activateHeaderControl(control, event);
  }

  function routeHeaderClick(event) {
    if (dispatchingHeaderClick) return;
    if (hasOpenDialog()) return;
    var directControl = headerControlFrom(event.target);
    var control = directControl || headerControlForEvent(event);
    if (control === lastActivatedHeaderControl && Date.now() <= suppressClickUntil) {
      lastActivatedHeaderControl = null;
      event.preventDefault();
      event.stopImmediatePropagation();
      return;
    }
    // A webOS overlay can own the click target even when the pointer is visibly
    // over a header button. Resolve that button from the event coordinates.
    if (!directControl) activateHeaderControl(control, event);
  }

  function selectPointedHeaderControl(event) {
    var key = event.key || event.keyCode;
    if (key === 'ArrowLeft' || key === 'ArrowRight'
        || key === 'ArrowUp' || key === 'ArrowDown'
        || key === 37 || key === 38 || key === 39 || key === 40) {
      lastPointer = null;
      return;
    }
    if (key !== 'Enter' && key !== 13) return;

    var control = headerControlFrom(document.activeElement);
    if (!control && lastPointer && Date.now() - lastPointer.at <= 6000) {
      control = headerControlAtPoint(lastPointer);
    }
    if (!control) return;
    focusControl(control);
    activateHeaderControl(control, event);
  }

  document.addEventListener('pointerover', focusHeaderControl, true);
  document.addEventListener('pointermove', focusHeaderControl, true);
  document.addEventListener('pointerdown', pressHeaderControl, true);
  document.addEventListener('pointerup', releaseHeaderControl, true);
  document.addEventListener('pointercancel', function () {
    pressedHeaderControl = null;
    pressedAt = null;
  }, true);
  document.addEventListener('mouseover', focusHeaderControl, true);
  document.addEventListener('mousemove', focusHeaderControl, true);
  document.addEventListener('mousedown', pressHeaderControl, true);
  document.addEventListener('mouseup', releaseHeaderControl, true);
  document.addEventListener('click', routeHeaderClick, true);
  document.addEventListener('keydown', selectPointedHeaderControl, true);
}());

// Jellyfin raises the video layer above the header before playback has started.
// Keep Back clickable, show server-reported startup progress, and route a
// cancellation through Jellyfin's own stop command before navigation.
(function () {
  var STARTING_CLASS = 'remux-playback-starting';
  var playStartedAt = 0;
  var playGeneration = 0;
  var statusTimer = null;
  var selectedSourceLabel = '';
  var sawStartupStatus = false;
  var startupUnavailableCount = 0;
  var startupFailed = false;

  var PHASE_LABELS = {
    preparing_playback: 'Preparing playback',
    starting_player: 'Starting the player',
    selecting_source: 'Finding a fast source',
    prebuffering: 'Building a playback buffer',
    opening_stream: 'Opening the selected stream',
    preparing_video: 'Preparing video',
    starting_playback: 'Starting playback'
  };

  function removeStartupStatus() {
    if (statusTimer !== null) {
      clearTimeout(statusTimer);
      statusTimer = null;
    }
    var status = document.querySelector('.remux-startup-status');
    if (status && status.parentNode) status.parentNode.removeChild(status);
  }

  function clearStarting() {
    document.documentElement.classList.remove(STARTING_CLASS);
    removeStartupStatus();
    startupFailed = false;
  }

  function selectedSource() {
    var selects = document.querySelectorAll('.selectSource');
    for (var i = 0; i < selects.length; i++) {
      if (selects[i].offsetParent !== null && selects[i].selectedIndex >= 0) {
        return selects[i].options[selects[i].selectedIndex].text;
      }
    }
    return '';
  }

  function ensureStartupStatus(allowWithoutPlayer) {
    var container = document.querySelector('.videoPlayerContainer-onTop');
    var status = document.querySelector('.remux-startup-status');
    if (!container && !allowWithoutPlayer) return null;
    if (status) {
      if (container && status.parentNode !== container) container.appendChild(status);
      return status;
    }

    status = document.createElement('div');
    status.className = 'remux-startup-status';
    status.innerHTML =
      '<div class="remux-startup-title" aria-live="polite"></div>' +
      '<div class="remux-startup-detail"></div>' +
      '<div class="remux-startup-progress" role="progressbar">' +
        '<div class="remux-startup-progress-bar remux-indeterminate"></div>' +
      '</div>' +
      '<div class="remux-startup-footnote"></div>';
    (container || document.body).appendChild(status);
    return status;
  }

  function formatBytes(bytes) {
    var value = Number(bytes) || 0;
    if (value >= 1073741824) return (value / 1073741824).toFixed(1) + ' GB';
    if (value >= 1048576) return Math.round(value / 1048576) + ' MB';
    if (value >= 1024) return Math.round(value / 1024) + ' KB';
    return Math.round(value) + ' bytes';
  }

  function formatRate(bitsPerSecond) {
    var value = Number(bitsPerSecond) || 0;
    if (value >= 1000000) return (value / 1000000).toFixed(1) + ' Mbps';
    if (value >= 1000) return Math.round(value / 1000) + ' Kbps';
    return '';
  }

  function formatWait(seconds) {
    var value = Math.max(0, Math.ceil(Number(seconds) || 0));
    if (value < 60) return value + 's';
    if (value < 3600) {
      return Math.floor(value / 60) + 'm ' + (value % 60) + 's';
    }
    return Math.floor(value / 3600) + 'h ' + Math.floor(value % 3600 / 60) + 'm';
  }

  function renderStartupStatus(startup) {
    var status = ensureStartupStatus(false);
    if (!status) return;

    status.classList.remove('remux-startup-error');
    var oldBack = status.querySelector('.remux-startup-back');
    if (oldBack && oldBack.parentNode) oldBack.parentNode.removeChild(oldBack);

    var phase = startup && startup.Phase || 'preparing_playback';
    var elapsed = startup && startup.ElapsedMilliseconds;
    if (elapsed == null) elapsed = Math.max(0, performance.now() - playStartedAt);
    var detail = selectedSourceLabel;
    var prebuffer = startup && startup.Prebuffer;
    var progress = null;
    var estimatedWait = null;

    if (prebuffer) {
      var downloaded = Number(prebuffer.DownloadedBytes) || 0;
      var target = Number(prebuffer.TargetBytes) || 0;
      if (prebuffer.RateTrusted && target > 0) {
        detail = formatBytes(downloaded) + ' of ' + formatBytes(target) + ' buffered';
        var rate = formatRate(prebuffer.EstimatedGoodputBitsPerSecond);
        if (rate) detail += ' · ' + rate;
        if (prebuffer.EstimatedWaitSeconds != null) {
          estimatedWait = Number(prebuffer.EstimatedWaitSeconds);
        }
        progress = Math.max(0, Math.min(100, downloaded / target * 100));
      } else {
        detail = formatBytes(downloaded) + ' buffered · measuring download speed';
      }
    } else if (phase === 'selecting_source') {
      detail = selectedSourceLabel
        ? selectedSourceLabel + ' · testing available sources'
        : 'Testing available sources';
    }

    status.querySelector('.remux-startup-title').textContent =
      PHASE_LABELS[phase] || PHASE_LABELS.preparing_playback;
    status.querySelector('.remux-startup-detail').textContent = detail || 'Preparing the selected version';
    var footnote = 'Elapsed ' + formatWait(elapsed / 1000);
    if (estimatedWait !== null) {
      footnote += ' · Estimated start in ' + formatWait(estimatedWait);
    } else if (prebuffer) {
      footnote += ' · Estimating start time';
    }
    status.querySelector('.remux-startup-footnote').textContent = footnote;

    var track = status.querySelector('.remux-startup-progress');
    var bar = status.querySelector('.remux-startup-progress-bar');
    if (progress === null) {
      bar.className = 'remux-startup-progress-bar remux-indeterminate';
      bar.style.width = '';
      track.removeAttribute('aria-valuenow');
    } else {
      bar.className = 'remux-startup-progress-bar';
      bar.style.width = progress.toFixed(1) + '%';
      track.setAttribute('aria-valuemin', '0');
      track.setAttribute('aria-valuemax', '100');
      track.setAttribute('aria-valuenow', String(Math.round(progress)));
    }
  }

  function urlIsNoStreams(value) {
    if (!value) return false;
    try {
      return new URL(String(value), location.href).pathname === '/videos/no-streams';
    } catch (error) {
      return false;
    }
  }

  function isNoStreamsPlayback(video) {
    if (video && (urlIsNoStreams(video.currentSrc) || urlIsNoStreams(video.getAttribute('src')))) {
      return true;
    }
    if (!window.performance || !performance.getEntriesByType) return false;
    var entries = performance.getEntriesByType('resource');
    for (var i = entries.length - 1; i >= 0; i--) {
      if (entries[i].startTime < playStartedAt - 1000) break;
      if (urlIsNoStreams(entries[i].name)) return true;
    }
    return false;
  }

  function renderStartupError() {
    if (!document.documentElement.classList.contains(STARTING_CLASS)) return;
    if (statusTimer !== null) {
      clearTimeout(statusTimer);
      statusTimer = null;
    }
    startupFailed = true;
    var apiClient = window.ApiClient;
    if (apiClient && typeof apiClient.stopActiveEncodings === 'function') {
      stopEncodingWhenAvailable(
        apiClient,
        playStartedAt,
        playGeneration,
        performance.now() + 30000
      );
    }
    var status = ensureStartupStatus(true);
    if (!status) return;
    status.classList.add('remux-startup-error');
    status.querySelector('.remux-startup-title').textContent = 'Unable to start video';
    status.querySelector('.remux-startup-detail').textContent =
      'Remux tried the available sources, but none became playable.';
    status.querySelector('.remux-startup-footnote').textContent =
      'Go back to choose another version or try again.';
    if (!status.querySelector('.remux-startup-back')) {
      var back = document.createElement('button');
      back.type = 'button';
      back.className = 'remux-startup-back';
      back.textContent = 'Back';
      status.appendChild(back);
    }
  }

  function currentPlaySessionId(startedAt) {
    var video = document.querySelector('video');
    if (video) {
      try {
        var videoId = new URL(
          video.currentSrc || video.getAttribute('src'),
          location.href
        ).searchParams.get('PlaySessionId');
        if (videoId) return videoId;
      } catch (error) {}
    }
    if (!window.performance || !performance.getEntriesByType) return null;
    var entries = performance.getEntriesByType('resource');
    for (var i = entries.length - 1; i >= 0; i--) {
      if (entries[i].startTime < startedAt - 1000) break;
      try {
        var id = new URL(entries[i].name, location.href).searchParams.get('PlaySessionId');
        if (id) return id;
      } catch (error) {}
    }
    return null;
  }

  function stopEncodingWhenAvailable(apiClient, startedAt, generation, deadline) {
    if (generation !== playGeneration) return;
    var playSessionId = currentPlaySessionId(startedAt);
    if (playSessionId) {
      apiClient.stopActiveEncodings(playSessionId).catch(function () {});
      return;
    }
    if (performance.now() < deadline) {
      setTimeout(function () {
        stopEncodingWhenAvailable(apiClient, startedAt, generation, deadline);
      }, 100);
    }
  }

  function scheduleStartupStatus(generation, delay) {
    statusTimer = setTimeout(function () {
      pollStartupStatus(generation);
    }, delay);
  }

  function fetchStartupStatus(apiClient) {
    var url = apiClient.getUrl('Remux/Playback/Startup', { _: Date.now() });
    return fetch(url, {
      cache: 'no-store',
      headers: { 'X-Emby-Token': apiClient.accessToken() }
    }).then(function (response) {
      if (!response.ok) throw new Error('startup status unavailable');
      return response.json();
    });
  }

  function clearStartupFailure(apiClient) {
    if (!apiClient || !window.fetch) return;
    var url = apiClient.getUrl('Remux/Playback/Startup');
    fetch(url, {
      method: 'DELETE',
      cache: 'no-store',
      headers: { 'X-Emby-Token': apiClient.accessToken() }
    }).catch(function () {});
  }

  function pollStartupStatus(generation) {
    if (generation !== playGeneration
        || startupFailed
        || !document.documentElement.classList.contains(STARTING_CLASS)) return;

    // Render the fallback only once. Replacing the last server-reported phase
    // before every poll makes the title flash between "Preparing playback"
    // and the real phase while the request is in flight.
    if (!document.querySelector('.remux-startup-status')) {
      renderStartupStatus(null);
    }
    var apiClient = window.ApiClient;
    if (!apiClient || !window.fetch) {
      scheduleStartupStatus(generation, 250);
      return;
    }

    fetchStartupStatus(apiClient).then(function (startup) {
      if (generation !== playGeneration
          || !document.documentElement.classList.contains(STARTING_CLASS)) return;
      if (startup && startup.Phase === 'failed') {
        renderStartupError();
        return;
      }
      sawStartupStatus = true;
      startupUnavailableCount = 0;
      renderStartupStatus(startup);
      scheduleStartupStatus(generation, 500);
    }, function () {
      if (generation === playGeneration
          && document.documentElement.classList.contains(STARTING_CLASS)) {
        startupUnavailableCount += 1;
        var video = document.querySelector('video');
        if (isNoStreamsPlayback(video)
            || (sawStartupStatus && startupUnavailableCount >= 2
                && (!video || video.paused || video.readyState < 2))) {
          renderStartupError();
        } else {
          scheduleStartupStatus(generation, 500);
        }
      }
    });
  }

  function clearWhenPlaybackChanges(event) {
    var target = event.target;
    if (!target || !target.matches || !target.matches('video')) return;
    if (event.type === 'emptied') {
      clearStarting();
      return;
    }
    var generation = playGeneration;
    setTimeout(function () {
      if (generation !== playGeneration || startupFailed) return;
      if (isNoStreamsPlayback(target)) {
        renderStartupError();
        return;
      }
      var apiClient = window.ApiClient;
      if (!apiClient || !window.fetch) {
        clearStarting();
        return;
      }
      fetchStartupStatus(apiClient).then(function (startup) {
        if (generation !== playGeneration || startupFailed) return;
        if (startup && startup.Phase === 'failed') renderStartupError();
        else clearStarting();
      }, function () {
        if (generation === playGeneration && !startupFailed) clearStarting();
      });
    }, 250);
  }

  document.addEventListener('playing', clearWhenPlaybackChanges, true);
  document.addEventListener('emptied', clearWhenPlaybackChanges, true);

  document.addEventListener('click', function (event) {
    var target = event.target;
    if (!target || !target.closest) return;

    if (target.closest('.btnPlay, .btnReplay')) {
      clearStartupFailure(window.ApiClient);
      removeStartupStatus();
      document.documentElement.classList.add(STARTING_CLASS);
      playStartedAt = performance.now();
      playGeneration += 1;
      selectedSourceLabel = selectedSource();
      sawStartupStatus = false;
      startupUnavailableCount = 0;
      startupFailed = false;
      scheduleStartupStatus(playGeneration, 0);
      return;
    }

    if (!document.documentElement.classList.contains(STARTING_CLASS)) return;
    var errorBack = target.closest('.remux-startup-back');
    var button = target.closest('.headerBackButton');
    if (errorBack) {
      var buttons = document.querySelectorAll('.headerBackButton');
      for (var i = 0; i < buttons.length; i++) {
        if (buttons[i].offsetParent !== null) {
          button = buttons[i];
          break;
        }
      }
    }
    if (!button && !errorBack) return;

    event.preventDefault();
    event.stopImmediatePropagation();
    var playerContainer = document.querySelector('.videoPlayerContainer-onTop');
    if (playerContainer) playerContainer.classList.remove('videoPlayerContainer-onTop');
    var video = playerContainer && playerContainer.querySelector('video');
    if (video) video.pause();
    var apiClient = window.ApiClient;
    if (apiClient && typeof apiClient.stopActiveEncodings === 'function') {
      stopEncodingWhenAvailable(
        apiClient,
        playStartedAt,
        playGeneration,
        performance.now() + 30000
      );
    }
    if (apiClient && typeof apiClient.sendPlayStateCommand === 'function') {
      apiClient.sendPlayStateCommand(apiClient.deviceId(), 'Stop').catch(function () {});
    }
    clearStartupFailure(apiClient);
    clearStarting();
    setTimeout(function () {
      if (button) button.click();
      else history.back();
    }, 0);
  }, true);
}());

// Search results already update on every input event. Enter therefore only
// needs to finish text entry; blur dismisses browser and webOS keyboards.
(function () {
  document.addEventListener('keydown', function (event) {
    var target = event.target;
    if (event.key !== 'Enter' || !target || target.id !== 'searchTextInput') return;
    setTimeout(function () { target.blur(); }, 0);
  }, true);
}());

// Strip "Recently Added in " prefix from homescreen section titles, leaving only the library name.
(function () {
  var PREFIX = 'Recently Added in ';

  function clean(el) {
    var walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT, null, false);
    var node;
    while ((node = walker.nextNode())) {
      if (node.nodeValue && node.nodeValue.indexOf(PREFIX) === 0) {
        node.nodeValue = node.nodeValue.slice(PREFIX.length);
      }
    }
  }

  function processRoot(root) {
    if (!root.querySelectorAll) return;
    var els = root.querySelectorAll('.sectionTitle, .sectionTitleLink');
    for (var i = 0; i < els.length; i++) clean(els[i]);
    if (root.classList && (root.classList.contains('sectionTitle') || root.classList.contains('sectionTitleLink'))) {
      clean(root);
    }
  }

  new MutationObserver(function (mutations) {
    for (var i = 0; i < mutations.length; i++) {
      var added = mutations[i].addedNodes;
      for (var j = 0; j < added.length; j++) {
        if (added[j].nodeType === 1) processRoot(added[j]);
      }
    }
  }).observe(document.body, { childList: true, subtree: true });

  processRoot(document.body);
}());

"#;

#[cfg(test)]
mod tests {
    use super::{CSS, JS};

    #[test]
    fn generic_item_transports_are_not_rewritten_to_source_stubs() {
        assert!(!JS.contains("proto.fetch = function"));
        assert!(!JS.contains("XMLHttpRequest.prototype.open = function"));
        assert!(
            JS.contains(
                "if (!isCurrentPage()) return _getItem.apply(self, arguments);"
            )
        );
    }

    #[test]
    fn async_sources_remain_scoped_to_their_detail_page() {
        assert!(JS.contains("item.MediaSources = ms;"));
        assert!(JS.contains("var ms = form._remuxMediaSources;"));
        assert!(!JS.contains("window._remuxCurrentMediaSources"));
    }

    #[test]
    fn webos_header_pointer_supports_pointer_mouse_and_select_events() {
        assert!(CSS.contains("html.layout-tv .skinHeader .headerTop"));
        assert!(CSS.contains("pointer-events: auto !important"));
        assert!(JS.contains("pressedHeaderControl = control;"));
        assert!(JS.contains("headerControlAtPoint"));
        assert!(JS.contains("addEventListener('pointerdown'"));
        assert!(JS.contains("selectPointedHeaderControl"));
        assert!(JS.contains("if (hasOpenDialog()) return null;"));
        assert!(
            JS.contains("var control = directControl || headerControlForEvent(event);")
        );
        assert!(JS.contains("control.click();"));
        assert!(JS.contains("routeHeaderClick"));
    }

    #[test]
    fn startup_status_shows_eta_without_a_cancellation_hint() {
        assert!(JS.contains("Estimated start in"));
        assert!(!JS.contains("Back cancels"));
    }

    #[test]
    fn startup_status_keeps_the_last_phase_between_polls() {
        assert!(JS.contains("if (!document.querySelector('.remux-startup-status'))"));
    }

    #[test]
    fn failed_playback_replaces_the_placeholder_video_with_an_actionable_error() {
        assert!(JS.contains("pathname === '/videos/no-streams'"));
        assert!(JS.contains("startup.Phase === 'failed'"));
        assert!(JS.contains("method: 'DELETE'"));
        assert!(JS.contains("clearStartupFailure(window.ApiClient);"));
        assert!(JS.contains("stopEncodingWhenAvailable("));
        assert!(JS.contains("Unable to start video"));
        assert!(JS.contains("Remux tried the available sources"));
        assert!(JS.contains("className = 'remux-startup-back'"));
        assert!(JS.contains("(container || document.body).appendChild(status);"));
        assert!(CSS.contains("position: fixed"));
        assert!(CSS.contains(".remux-startup-status.remux-startup-error"));
        assert!(CSS.contains("pointer-events: auto"));
    }

    #[test]
    fn startup_progress_waits_for_the_player_view() {
        assert!(JS.contains("if (!container && !allowWithoutPlayer) return null;"));
        assert!(JS.contains("var status = ensureStartupStatus(false);"));
        assert!(JS.contains("var status = ensureStartupStatus(true);"));
    }
}
