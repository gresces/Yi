(function () {
  'use strict';

  var REQUEST_TIMEOUT_MS = 8000;
  var STATUS_HOLD_MS = 3000;
  var STATUS_FADE_MS = 220;
  var TEXTAREA_MIN_HEIGHT = 40;
  var TEXTAREA_MAX_HEIGHT = 82;

  var PAGE_SIZE_KEY = 'yi.events.pageSize';
  var PAGE_SIZES = [25, 50, 100, 200];
  var DEFAULT_PAGE_SIZE = 50;
  var FAILURE_PAGE_SIZE = 50;
  var NEW_DATA_DEBOUNCE_MS = 400;

  var REL_TIME_TICK_MS = 30 * 1000;
  var PRODUCER_LIMIT = 6;
  var PRODUCER_ACTIVE_MS = 2 * 60 * 1000;
  var CONSUMER_STALE_MS = 5 * 60 * 1000;

  var isMac = /Mac|iPhone|iPad|iPod/i.test(
    (navigator.platform || '') + ' ' + (navigator.userAgent || '')
  );
  var finePointer = window.matchMedia('(pointer: fine)');
  var narrowScreen = window.matchMedia('(max-width: 767px)');
  var numberFormatter = new Intl.NumberFormat('zh-CN');

  var CONNECTION_LABELS = {
    connecting: '连接中',
    online: '在线',
    reconnecting: '重连中',
    unsupported: '不可用'
  };

  var el = {};
  var state = {
    view: 'overview',

    /* 快照 */
    events: [],
    typeCounts: [],
    total: 0,
    consumers: [],
    producers: [],
    failureTotal: 0,
    hasSnapshot: false,
    streamFailed: false,
    unsupported: false,
    connection: 'connecting',
    consumerKey: '',
    producerKey: '',
    lastSnapshotTotal: null,

    /* 相对时间 */
    relTimeTimer: null,

    /* 采集 */
    captureBusy: false,
    statusTimer: null,
    statusFadeTimer: null,

    /* 事件页 */
    pageSize: readStoredPageSize(),
    page: 1,
    selectedType: '',
    pageEvents: [],
    pageTotal: 0,
    pageLoaded: false,
    pageLoading: false,
    userLoading: false,
    loadedPage: 1,
    loadedType: '',
    loadedSize: DEFAULT_PAGE_SIZE,
    pageRequestSeq: 0,
    pageRetryTarget: null,
    pageFocus: '',
    pageRenderKey: '',
    renderedIds: new Set(),
    hasRenderedPage: false,
    /* 回到第 1 页时已经覆盖到的全局事件总数,用于判断“有新数据” */
    seenGlobalTotal: null,
    newDataTimer: null,
    filterSignature: '',

    /* 失败页 */
    failurePage: 1,
    failurePageTotal: 0,
    failurePageRows: [],
    failurePageLoaded: false,
    failurePageLoading: false,
    failureUserLoading: false,
    failureLoadedPage: 1,
    failurePageRequestSeq: 0,
    failurePageRetryTarget: null,
    failurePageFocus: '',
    failurePageRenderKey: '',
    /* 第 1 页已经覆盖到的失败总数,用于判断“有新数据” */
    failureSeenTotal: null,
    failureNewDataTimer: null
  };

  /* ---- 启动 ---- */

  function init() {
    el.skipLink = document.querySelector('.skip-link');
    el.pageTitle = document.getElementById('page-title');
    el.pageSub = document.getElementById('page-sub');
    el.viewOverview = document.getElementById('view-overview');
    el.viewEvents = document.getElementById('view-events');
    el.viewFailures = document.getElementById('view-failures');

    el.captureForm = document.getElementById('capture-form');
    el.captureText = document.getElementById('capture-text');
    el.captureSubmit = document.getElementById('capture-submit');
    el.captureStatus = document.getElementById('capture-status');
    el.captureError = document.getElementById('capture-error');
    el.captureErrorText = document.getElementById('capture-error-text');
    el.captureTitle = document.getElementById('capture-title');
    el.keyMod = document.getElementById('key-mod');

    el.connection = document.getElementById('connection');
    el.connectionLabel = document.getElementById('connection-label');
    el.sidebarStatus = document.getElementById('sidebar-status');
    el.sidebarConnectionLabel = document.getElementById('sidebar-connection-label');

    el.streamNote = document.getElementById('stream-note');
    el.streamNoteText = document.getElementById('stream-note-text');

    el.statEvents = document.getElementById('stat-events');
    el.statConsumers = document.getElementById('stat-consumers');
    el.statPending = document.getElementById('stat-pending');
    el.statFailures = document.getElementById('stat-failures');
    el.kpiFailures = document.getElementById('kpi-failures');

    el.eventsPanel = document.getElementById('events');
    el.eventsTitle = document.getElementById('events-title');
    el.eventsCount = document.getElementById('events-count');
    el.eventsPageInfo = document.getElementById('events-page-info');
    el.eventsStatus = document.getElementById('events-status');
    el.filters = document.getElementById('filters');
    el.pageSize = document.getElementById('page-size');
    el.eventTable = document.getElementById('event-table');
    el.eventRows = document.getElementById('event-rows');
    el.tableWrap = document.getElementById('events-table-wrap');
    el.eventsSkeleton = document.getElementById('events-skeleton');
    el.eventsEmpty = document.getElementById('events-empty');
    el.eventsEmptyTitle = document.getElementById('events-empty-title');
    el.eventsEmptyText = document.getElementById('events-empty-text');
    el.eventsError = document.getElementById('events-error');
    el.eventsErrorText = document.getElementById('events-error-text');
    el.eventsNewData = document.getElementById('events-new-data');
    el.eventsInlineError = document.getElementById('events-inline-error');
    el.eventsInlineErrorText = document.getElementById('events-inline-error-text');
    el.eventsRetry = document.getElementById('events-retry');
    el.eventsPagination = document.getElementById('events-pagination');
    el.paginationTotal = document.getElementById('pagination-total');
    el.paginationPage = document.getElementById('pagination-page');
    el.paginationPageValue = document.getElementById('pagination-page-value');
    el.eventsPrev = document.getElementById('events-prev');
    el.eventsNext = document.getElementById('events-next');

    el.failuresPanel = document.getElementById('failures');
    el.failuresTitle = document.getElementById('failures-title');
    el.failuresCount = document.getElementById('failures-count');
    el.failuresPageInfo = document.getElementById('failures-page-info');
    el.failuresStatus = document.getElementById('failures-status');
    el.failuresNewData = document.getElementById('failures-new-data');
    el.failuresInlineError = document.getElementById('failures-inline-error');
    el.failuresInlineErrorText = document.getElementById('failures-inline-error-text');
    el.failuresRetry = document.getElementById('failures-retry');
    el.failuresTable = document.getElementById('failure-table');
    el.failureRows = document.getElementById('failure-rows');
    el.failuresTableWrap = document.getElementById('failures-table-wrap');
    el.failuresSkeleton = document.getElementById('failures-skeleton');
    el.failuresEmpty = document.getElementById('failures-empty');
    el.failuresEmptyTitle = document.getElementById('failures-empty-title');
    el.failuresEmptyText = document.getElementById('failures-empty-text');
    el.failuresError = document.getElementById('failures-error');
    el.failuresErrorText = document.getElementById('failures-error-text');
    el.failuresPagination = document.getElementById('failures-pagination');
    el.failuresPaginationTotal = document.getElementById('failures-pagination-total');
    el.failuresPaginationPage = document.getElementById('failures-pagination-page');
    el.failuresPageValue = document.getElementById('failures-page-value');
    el.failuresPrev = document.getElementById('failures-prev');
    el.failuresNext = document.getElementById('failures-next');

    el.consumersPanel = document.getElementById('consumers');
    el.consumersCount = document.getElementById('consumers-count');
    el.consumerList = document.getElementById('consumer-list');
    el.consumersSkeleton = document.getElementById('consumers-skeleton');
    el.consumersEmpty = document.getElementById('consumers-empty');
    el.consumersEmptyTitle = document.getElementById('consumers-empty-title');
    el.consumersEmptyText = document.getElementById('consumers-empty-text');

    el.producersPanel = document.getElementById('producers');
    el.producersCount = document.getElementById('producers-count');
    el.producerList = document.getElementById('producer-list');
    el.producersSkeleton = document.getElementById('producers-skeleton');
    el.producersEmpty = document.getElementById('producers-empty');
    el.producersEmptyTitle = document.getElementById('producers-empty-title');
    el.producersEmptyText = document.getElementById('producers-empty-text');
    el.producersMore = document.getElementById('producers-more');

    el.keyMod.textContent = isMac ? 'Cmd' : 'Ctrl';
    el.pageSize.value = String(state.pageSize);

    bindEvents();
    initRouter();
    syncCaptureSubmit();
    autosize();
    setConnection('connecting');
    updateStreamNote();
    startRelativeTimeTicker();

    if (typeof window.EventSource === 'function') {
      startStream();
    } else {
      handleUnsupportedStream();
    }
  }

  function bindEvents() {
    el.captureForm.addEventListener('submit', function (event) {
      event.preventDefault();
      submitCapture();
    });

    el.captureText.addEventListener('input', function () {
      syncCaptureSubmit();
      autosize();
      if (!el.captureError.hidden) {
        hideCaptureError();
      }
    });

    el.captureText.addEventListener('keydown', function (event) {
      if (event.key !== 'Enter' || !(event.ctrlKey || event.metaKey)) {
        return;
      }
      if (event.isComposing || event.keyCode === 229) {
        return;
      }
      event.preventDefault();
      submitCapture();
    });

    el.skipLink.addEventListener('click', function (event) {
      event.preventDefault();
      var target;
      if (state.view === 'events') {
        target = el.eventsTitle;
      } else if (state.view === 'failures') {
        target = el.failuresTitle;
      } else {
        target = el.captureTitle;
      }
      if (target) {
        target.focus();
      }
    });

    el.filters.addEventListener('click', function (event) {
      if (!(event.target instanceof Element)) {
        return;
      }
      var chip = event.target.closest('.filter-chip');
      if (!chip || !el.filters.contains(chip)) {
        return;
      }
      selectType(chip.getAttribute('data-type') || '');
    });

    el.eventsPrev.addEventListener('click', function () {
      goToPage(state.page - 1);
    });

    el.eventsNext.addEventListener('click', function () {
      goToPage(state.page + 1);
    });

    el.eventsNewData.addEventListener('click', handleNewDataClick);
    el.eventsRetry.addEventListener('click', retryPageLoad);
    el.pageSize.addEventListener('change', handlePageSizeChange);

    el.failuresPrev.addEventListener('click', function () {
      goToFailurePage(state.failurePage - 1);
    });

    el.failuresNext.addEventListener('click', function () {
      goToFailurePage(state.failurePage + 1);
    });

    el.failuresNewData.addEventListener('click', handleFailureNewDataClick);
    el.failuresRetry.addEventListener('click', retryFailurePageLoad);

    document.addEventListener('visibilitychange', handleVisibilityChange);
    window.addEventListener('resize', handleWindowResize);
  }

  function handleWindowResize() {
    autosize();
  }

  /* ---- 视图路由 ---- */

  function initRouter() {
    window.addEventListener('hashchange', handleHashChange);

    var route = readRoute();
    applyView(route, true);
    normalizeRoute(route);
  }

  function readRoute() {
    var hash = window.location.hash;
    if (hash === '#/events') {
      return 'events';
    }
    if (hash === '#/failures') {
      return 'failures';
    }
    return 'overview';
  }

  function handleHashChange() {
    var route = readRoute();
    applyView(route, false);
    normalizeRoute(route);
  }

  function normalizeRoute(view) {
    var want = '#/' + view;
    if (window.location.hash === want) {
      return;
    }
    try {
      window.history.replaceState(null, '', want);
    } catch (error) {
      window.location.hash = want;
    }
  }

  function applyView(view, initial) {
    var changed = state.view !== view;
    state.view = view;

    el.viewOverview.hidden = view !== 'overview';
    el.viewEvents.hidden = view !== 'events';
    el.viewFailures.hidden = view !== 'failures';
    updateNav(view);
    updateViewChrome(view);

    if (view === 'events') {
      state.failurePageFocus = '';
      cancelFailureAutoRefresh();
      ensureEventsView();
    } else if (view === 'failures') {
      state.pageFocus = '';
      cancelAutoRefresh();
      updateNewDataHint();
      ensureFailuresView();
    } else {
      cancelAutoRefresh();
      cancelFailureAutoRefresh();
      state.pageFocus = '';
      state.failurePageFocus = '';
      updateNewDataHint();
      updateFailureNewDataHint();
      autosize();
    }

    if (changed && !initial) {
      window.scrollTo(0, 0);
      el.pageTitle.focus({ preventScroll: true });
    }

    return changed;
  }

  function updateNav(view) {
    var links = document.querySelectorAll('[data-view]');
    for (var i = 0; i < links.length; i += 1) {
      if (links[i].getAttribute('data-view') === view) {
        links[i].setAttribute('aria-current', 'page');
      } else {
        links[i].removeAttribute('aria-current');
      }
    }
  }

  function updateViewChrome(view) {
    var title = '概览';
    var sub = '实时事件流与消费者进度';

    if (view === 'events') {
      title = '事件';
      sub = '按类型筛选，逐页浏览全部事件';
    } else if (view === 'failures') {
      title = '失败记录';
      sub = '被放弃的事件与错误原因';
    }

    if (el.pageTitle.textContent !== title) {
      el.pageTitle.textContent = title;
    }
    if (el.pageSub.textContent !== sub) {
      el.pageSub.textContent = sub;
    }

    var docTitle = 'Yi · ' + title;
    if (document.title !== docTitle) {
      document.title = docTitle;
    }
  }

  /* ---- 采集 ---- */

  function syncCaptureSubmit() {
    el.captureSubmit.disabled = state.captureBusy || el.captureText.value.trim() === '';
  }

  function autosize() {
    var textarea = el.captureText;
    if (!textarea) {
      return;
    }
    textarea.style.height = 'auto';
    var borders = textarea.offsetHeight - textarea.clientHeight;
    var naturalHeight = textarea.scrollHeight + (borders > 0 ? borders : 0);
    var nextHeight = Math.min(
      Math.max(naturalHeight, TEXTAREA_MIN_HEIGHT),
      TEXTAREA_MAX_HEIGHT
    );
    textarea.style.height = nextHeight + 'px';
    textarea.style.overflowY = naturalHeight > TEXTAREA_MAX_HEIGHT ? 'auto' : 'hidden';
  }

  async function submitCapture() {
    if (state.captureBusy) {
      return;
    }

    var text = el.captureText.value.trim();
    if (!text) {
      syncCaptureSubmit();
      return;
    }

    state.captureBusy = true;
    hideCaptureError();
    clearCaptureStatus();
    el.captureSubmit.disabled = true;
    el.captureSubmit.setAttribute('aria-busy', 'true');
    el.captureSubmit.textContent = '记录中';
    el.captureForm.setAttribute('aria-busy', 'true');

    try {
      var result = await requestJSON('/api/capture', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({ text: text, source: 'web' })
      });

      if (el.captureText.value.trim() === text) {
        el.captureText.value = '';
        autosize();
      }

      showCaptureStatus(result && result.duplicate ? 'duplicate' : 'success');
      if (finePointer.matches) {
        el.captureText.focus({ preventScroll: true });
      }
    } catch (error) {
      if (error && error.detail) {
        console.warn('capture failed:', error.status || '', error.detail);
      }
      showCaptureError(captureFailureMessage(error));
      if (finePointer.matches) {
        el.captureText.focus({ preventScroll: true });
      }
    } finally {
      state.captureBusy = false;
      el.captureSubmit.textContent = '记录';
      el.captureSubmit.removeAttribute('aria-busy');
      el.captureForm.removeAttribute('aria-busy');
      syncCaptureSubmit();
    }
  }

  function captureFailureMessage(error) {
    if (error && error.timeout) {
      return '请求超时，这次没有记下。请稍后再试。';
    }
    if (error && error.network) {
      return '连不上服务，这次没有记下。请检查连接后再试。';
    }
    if (error && error.status === 413) {
      return '内容太长，这次没有记下。';
    }
    if (error && (error.status === 400 || error.status === 422)) {
      return '内容没有通过检查，请修改后再试。';
    }
    if (error && error.status >= 500) {
      return '服务暂时出了问题，这次没有记下。';
    }
    return '这次没有记下，请稍后再试。';
  }

  function showCaptureError(message) {
    el.captureErrorText.textContent = message;
    el.captureError.hidden = false;
  }

  function hideCaptureError() {
    el.captureErrorText.textContent = '';
    el.captureError.hidden = true;
  }

  function showCaptureStatus(kind) {
    clearStatusTimers();
    el.captureStatus.classList.remove('is-fading');
    el.captureStatus.dataset.state = kind;
    el.captureStatus.textContent = kind === 'duplicate' ? '已记录过' : '已记录';

    state.statusTimer = window.setTimeout(function () {
      state.statusTimer = null;
      el.captureStatus.classList.add('is-fading');
      state.statusFadeTimer = window.setTimeout(function () {
        state.statusFadeTimer = null;
        el.captureStatus.textContent = '';
        el.captureStatus.removeAttribute('data-state');
        el.captureStatus.classList.remove('is-fading');
      }, STATUS_FADE_MS);
    }, STATUS_HOLD_MS);
  }

  function clearCaptureStatus() {
    clearStatusTimers();
    el.captureStatus.classList.remove('is-fading');
    el.captureStatus.textContent = '';
    el.captureStatus.removeAttribute('data-state');
  }

  function clearStatusTimers() {
    if (state.statusTimer !== null) {
      window.clearTimeout(state.statusTimer);
      state.statusTimer = null;
    }
    if (state.statusFadeTimer !== null) {
      window.clearTimeout(state.statusFadeTimer);
      state.statusFadeTimer = null;
    }
  }

  /* ---- 实时推送 ---- */

  function startStream() {
    var source = new EventSource('/api/stream');
    source.addEventListener('open', handleStreamOpen);
    source.addEventListener('snapshot', handleSnapshot);
    source.addEventListener('error', handleStreamError);
  }

  function handleStreamOpen() {
    state.streamFailed = false;
    setConnection('online');
    if (!state.hasSnapshot) {
      showSideLoading();
    }
    updateStreamNote();
  }

  function handleStreamError() {
    state.streamFailed = true;
    setConnection('reconnecting');
    if (!state.hasSnapshot) {
      showSideWaiting();
    }
    updateStreamNote();
  }

  function handleUnsupportedStream() {
    state.unsupported = true;
    state.streamFailed = true;
    setConnection('unsupported');
    showSideWaiting();
    updateStreamNote();
  }

  function handleSnapshot(message) {
    var payload = parsePayload(message.data);
    if (!payload) {
      return;
    }

    var previousFailureTotal = state.failureTotal;
    var hadSnapshot = state.hasSnapshot;

    state.hasSnapshot = true;
    state.streamFailed = false;
    applySnapshot(payload);
    setConnection('online');
    updateStreamNote();
    handleFailureSnapshot(hadSnapshot ? previousFailureTotal : null);
    refreshRelativeTimes();
  }

  function parsePayload(raw) {
    if (typeof raw !== 'string' || raw === '') {
      return null;
    }
    try {
      var value = JSON.parse(raw);
      return isRecord(value) ? value : null;
    } catch (error) {
      return null;
    }
  }

  function applySnapshot(payload) {
    var events = normalizeEvents(payload.events);
    var total = Number(payload.total);
    if (!Number.isFinite(total) || total < 0) {
      total = events.length;
    }
    total = Math.max(Math.floor(total), events.length);

    var previousTotal = state.lastSnapshotTotal;

    var snapshotFailureCount = 0;
    if (Array.isArray(payload.failures)) {
      payload.failures.forEach(function (value) {
        if (isRecord(value)) {
          snapshotFailureCount += 1;
        }
      });
    }
    var failureTotal = Number(payload.failure_total);
    if (!Number.isFinite(failureTotal) || failureTotal < 0) {
      failureTotal = snapshotFailureCount;
    }
    failureTotal = Math.max(Math.floor(failureTotal), snapshotFailureCount);

    state.events = events;
    state.typeCounts = normalizeTypeCounts(payload.types);
    state.total = total;
    state.consumers = Array.isArray(payload.consumers)
      ? payload.consumers.filter(isRecord)
      : [];
    state.producers = normalizeProducers(payload.producers);
    state.failureTotal = failureTotal;
    state.lastSnapshotTotal = total;

    applyKpis();
    syncFilters();
    renderProducers();
    renderConsumers();

    if (state.view === 'events' && state.pageLoaded) {
      var increased = previousTotal === null
        ? hasUnseenEvents()
        : total > previousTotal;

      if (increased &&
          !state.pageLoading &&
          state.page === 1 &&
          !state.selectedType) {
        scheduleEventsRefresh();
      }
      updateNewDataHint();
    }
  }

  function applyKpis() {
    var pending = state.consumers.reduce(function (sum, consumer) {
      var value = toCount(consumer.pending);
      return sum + (value === null ? 0 : value);
    }, 0);

    el.statEvents.textContent = formatNumber(state.total);
    el.statConsumers.textContent = formatNumber(state.consumers.length);
    el.statPending.textContent = formatNumber(pending);
    el.statFailures.textContent = formatNumber(state.failureTotal);
    el.kpiFailures.classList.toggle('is-negative', state.failureTotal > 0);
  }

  /* ---- 生产者与相对时间 ---- */

  function normalizeProducers(raw) {
    if (!Array.isArray(raw)) {
      return [];
    }
    var rows = [];
    raw.forEach(function (value) {
      if (!isRecord(value)) {
        return;
      }
      var source = typeof value.source === 'string' ? value.source.trim() : '';
      if (!source) {
        return;
      }
      var count = Number(value.count);
      rows.push({
        source: source,
        count: Number.isFinite(count) && count > 0 ? Math.floor(count) : 0,
        lastAt: typeof value.last_at === 'string' ? value.last_at : ''
      });
    });
    rows.sort(function (a, b) {
      if (a.count !== b.count) {
        return b.count - a.count;
      }
      return a.source < b.source ? -1 : a.source > b.source ? 1 : 0;
    });
    return rows;
  }

  function renderProducers() {
    el.producersSkeleton.hidden = true;
    el.producersPanel.setAttribute('aria-busy', 'false');
    updateProducersCount();

    if (state.producers.length === 0) {
      el.producerList.textContent = '';
      el.producerList.hidden = true;
      el.producersMore.hidden = true;
      el.producersMore.textContent = '';
      el.producersEmpty.hidden = false;
      el.producersEmptyTitle.textContent = '还没有生产者';
      el.producersEmptyText.textContent = '发布事件后，这里会显示输入来源。';
      state.producerKey = '';
      return;
    }

    el.producersEmpty.hidden = true;
    el.producerList.hidden = false;

    var key = state.producers.map(function (producer) {
      return producer.source + '|' + producer.count + '|' + producer.lastAt;
    }).join('\u0000');

    if (key === state.producerKey) {
      return;
    }
    state.producerKey = key;

    var shown = state.producers.slice(0, PRODUCER_LIMIT);
    var fragment = document.createDocumentFragment();
    shown.forEach(function (producer) {
      fragment.appendChild(createProducerRow(producer));
    });
    el.producerList.textContent = '';
    el.producerList.appendChild(fragment);

    var extra = state.producers.length - shown.length;
    if (extra > 0) {
      el.producersMore.textContent = '+' + extra + ' 个来源';
      el.producersMore.hidden = false;
    } else {
      el.producersMore.textContent = '';
      el.producersMore.hidden = true;
    }
  }

  function createProducerRow(producer) {
    var row = document.createElement('li');
    row.className = 'producer-row';

    var active = isRecent(producer.lastAt, PRODUCER_ACTIVE_MS);
    var status = document.createElement('span');
    status.className = 'producer-state';
    status.setAttribute('data-state', active ? 'active' : 'idle');
    if (producer.lastAt) {
      status.setAttribute('data-at', producer.lastAt);
    }
    status.setAttribute('title', active ? '最近 2 分钟内有新事件' : '最近 2 分钟没有新事件');

    var dot = document.createElement('span');
    dot.className = 'status-dot';
    dot.setAttribute('aria-hidden', 'true');
    status.appendChild(dot);

    var statusLabel = document.createElement('span');
    statusLabel.className = 'producer-state-label';
    statusLabel.textContent = active ? '活跃' : '静默';
    status.appendChild(statusLabel);
    row.appendChild(status);

    var name = document.createElement('span');
    name.className = 'producer-name';
    name.textContent = producer.source;
    name.setAttribute('title', producer.source);
    row.appendChild(name);

    var count = document.createElement('span');
    count.className = 'producer-count';
    count.textContent = formatNumber(producer.count);
    var unit = document.createElement('span');
    unit.className = 'producer-count-unit';
    unit.textContent = '条';
    count.appendChild(unit);
    row.appendChild(count);

    var time = document.createElement('time');
    time.className = 'producer-time rel-time';
    if (producer.lastAt) {
      time.setAttribute('datetime', producer.lastAt);
      time.setAttribute('data-at', producer.lastAt);
    }
    var lastText = producer.lastAt ? relativeTime(producer.lastAt) : '';
    time.textContent = lastText || '—';
    row.appendChild(time);

    return row;
  }

  function updateProducersCount() {
    if (!state.hasSnapshot) {
      return;
    }
    if (state.producers.length === 0) {
      el.producersCount.textContent = '尚未有来源';
    } else {
      el.producersCount.textContent = formatNumber(state.producers.length) + ' 个来源';
    }
  }

  function parseTimeValue(raw) {
    if (typeof raw !== 'string' || raw === '') {
      return NaN;
    }
    var value = Date.parse(raw);
    return Number.isNaN(value) ? NaN : value;
  }

  function relativeTime(raw) {
    var time = parseTimeValue(raw);
    if (Number.isNaN(time)) {
      return '';
    }
    var seconds = Math.floor(Math.max(0, Date.now() - time) / 1000);
    if (seconds < 5) {
      return '刚刚';
    }
    if (seconds < 60) {
      return seconds + ' 秒前';
    }
    var minutes = Math.floor(seconds / 60);
    if (minutes < 60) {
      return minutes + ' 分钟前';
    }
    var hours = Math.floor(minutes / 60);
    if (hours < 24) {
      return hours + ' 小时前';
    }
    return Math.floor(hours / 24) + ' 天前';
  }

  function isRecent(raw, threshold) {
    var time = parseTimeValue(raw);
    if (Number.isNaN(time)) {
      return false;
    }
    var delta = Date.now() - time;
    return delta <= threshold && delta >= -60000;
  }

  function startRelativeTimeTicker() {
    if (state.relTimeTimer !== null) {
      return;
    }
    state.relTimeTimer = window.setInterval(refreshRelativeTimes, REL_TIME_TICK_MS);
  }

  function stopRelativeTimeTicker() {
    if (state.relTimeTimer !== null) {
      window.clearInterval(state.relTimeTimer);
      state.relTimeTimer = null;
    }
  }

  function handleVisibilityChange() {
    if (document.hidden) {
      stopRelativeTimeTicker();
      return;
    }
    startRelativeTimeTicker();
    refreshRelativeTimes();
  }

  function refreshRelativeTimes() {
    var nodes = document.querySelectorAll('.rel-time[data-at]');
    for (var i = 0; i < nodes.length; i += 1) {
      var raw = nodes[i].getAttribute('data-at') || '';
      var text = relativeTime(raw);
      if (text && nodes[i].textContent !== text) {
        nodes[i].textContent = text;
      }
    }
  }

  /* ---- 事件页 ---- */

  function readStoredPageSize() {
    try {
      var raw = window.localStorage.getItem(PAGE_SIZE_KEY);
      var size = Number(raw);
      if (PAGE_SIZES.indexOf(size) !== -1) {
        return size;
      }
    } catch (error) {
      /* 隐私模式下 localStorage 可能不可用 */
    }
    return DEFAULT_PAGE_SIZE;
  }

  function storePageSize(size) {
    try {
      window.localStorage.setItem(PAGE_SIZE_KEY, String(size));
    } catch (error) {
      /* 忽略写入失败 */
    }
  }

  function pageCount(total, size) {
    var count = Number(total);
    var perPage = Number(size);
    if (!Number.isFinite(perPage) || perPage <= 0) {
      perPage = state.pageSize || DEFAULT_PAGE_SIZE;
    }
    if (!Number.isFinite(count) || count <= 0) {
      return 1;
    }
    return Math.max(1, Math.ceil(count / perPage));
  }

  function hasUnseenEvents() {
    return state.seenGlobalTotal !== null &&
      state.lastSnapshotTotal !== null &&
      state.lastSnapshotTotal > state.seenGlobalTotal;
  }

  function ensureEventsView() {
    if (!state.pageLoaded) {
      if (!state.pageLoading) {
        loadEventsPage();
      }
      return;
    }

    if (!state.pageLoading &&
        state.page === 1 &&
        !state.selectedType &&
        hasUnseenEvents()) {
      scheduleEventsRefresh(0);
    }

    updateNewDataHint();
  }

  function scheduleEventsRefresh(delay) {
    if (state.view !== 'events' || !state.pageLoaded) {
      return;
    }

    cancelAutoRefresh();

    var wait = typeof delay === 'number' ? delay : NEW_DATA_DEBOUNCE_MS;
    state.newDataTimer = window.setTimeout(function () {
      state.newDataTimer = null;

      if (state.view !== 'events' || state.pageLoading || !state.pageLoaded) {
        return;
      }
      if (state.page !== 1 || state.selectedType) {
        updateNewDataHint();
        return;
      }
      loadEventsPage({ silent: true });
    }, wait);

    updateNewDataHint();
  }

  function cancelAutoRefresh() {
    if (state.newDataTimer !== null) {
      window.clearTimeout(state.newDataTimer);
      state.newDataTimer = null;
    }
  }

  function updateNewDataHint() {
    var autoPending = state.newDataTimer !== null &&
      state.page === 1 &&
      !state.selectedType;

    var show = state.view === 'events' &&
      state.pageLoaded &&
      !state.pageLoading &&
      !autoPending &&
      el.eventsInlineError.hidden &&
      hasUnseenEvents();

    el.eventsNewData.hidden = !show;

    var message = show ? '有新数据，可回到第 1 页刷新。' : '';
    if (el.eventsStatus.textContent !== message) {
      el.eventsStatus.textContent = message;
    }
  }

  function updateFilterPressed() {
    var chips = el.filters.querySelectorAll('.filter-chip');
    for (var i = 0; i < chips.length; i += 1) {
      var pressed = (chips[i].getAttribute('data-type') || '') === state.selectedType;
      chips[i].setAttribute('aria-pressed', pressed ? 'true' : 'false');
    }
  }

  function setFilterDisabled(disabled) {
    var chips = el.filters.querySelectorAll('.filter-chip');
    for (var i = 0; i < chips.length; i += 1) {
      chips[i].disabled = disabled;
    }
  }

  function selectType(type) {
    if (type === state.selectedType) {
      return;
    }

    cancelAutoRefresh();
    cancelPageLoad();

    state.selectedType = type;
    state.page = 1;
    state.pageRetryTarget = null;
    state.pageFocus = 'filter';
    hideInlineError();
    syncFilters();
    loadEventsPage();
  }

  function handlePageSizeChange() {
    var next = Number(el.pageSize.value);
    if (PAGE_SIZES.indexOf(next) === -1 || next === state.pageSize) {
      return;
    }

    cancelAutoRefresh();
    cancelPageLoad();

    state.pageSize = next;
    state.page = 1;
    state.pageRetryTarget = null;
    state.pageFocus = 'size';
    hideInlineError();
    storePageSize(next);
    loadEventsPage();
  }

  function goToPage(page) {
    if (!state.pageLoaded || state.userLoading) {
      return;
    }

    var pages = pageCount(state.pageTotal, state.pageSize);
    var target = Math.min(Math.max(1, Math.floor(page)), pages);
    if (target === state.page) {
      return;
    }

    var active = document.activeElement;
    cancelAutoRefresh();
    cancelPageLoad();

    state.pageFocus = active === el.eventsNext
      ? 'next'
      : active === el.eventsPrev ? 'prev' : '';
    state.page = target;
    state.pageRetryTarget = null;
    hideInlineError();
    loadEventsPage();
  }

  function handleNewDataClick() {
    if (!state.pageLoaded || state.pageLoading) {
      return;
    }

    cancelAutoRefresh();
    cancelPageLoad();

    state.page = 1;
    state.pageRetryTarget = null;
    state.pageFocus = 'title';
    hideInlineError();
    loadEventsPage();
  }

  function retryPageLoad() {
    var target = state.pageRetryTarget;
    if (!target) {
      return;
    }

    cancelAutoRefresh();
    cancelPageLoad();

    state.page = target.page;
    state.selectedType = target.type;
    state.pageSize = target.size;
    el.pageSize.value = String(target.size);
    state.pageRetryTarget = null;
    state.pageFocus = 'title';
    hideInlineError();
    syncFilters();
    loadEventsPage();
  }

  function cancelPageLoad() {
    state.pageRequestSeq += 1;
    state.pageLoading = false;
    state.userLoading = false;
  }

  function restorePageFocus() {
    var side = state.pageFocus;
    state.pageFocus = '';
    if (!side) {
      return;
    }

    var active = document.activeElement;

    if (side === 'title') {
      if (active && active !== document.body &&
          active !== el.eventsNewData && active !== el.eventsRetry &&
          active !== el.eventsPrev && active !== el.eventsNext) {
        return;
      }
      el.eventsTitle.focus({ preventScroll: true });
      return;
    }

    if (side === 'size') {
      if (active && active !== document.body && active !== el.pageSize) {
        return;
      }
      if (!el.pageSize.disabled) {
        el.pageSize.focus({ preventScroll: true });
      }
      return;
    }

    if (side === 'filter') {
      var pressed = el.filters.querySelector('.filter-chip[aria-pressed="true"]');
      if (!pressed || pressed.disabled) {
        return;
      }
      if (active && active !== document.body && !el.filters.contains(active)) {
        return;
      }
      pressed.focus({ preventScroll: true });
      return;
    }

    var primary = side === 'next' ? el.eventsNext : el.eventsPrev;
    var fallback = side === 'next' ? el.eventsPrev : el.eventsNext;

    if (active && active !== document.body &&
        active !== primary && active !== fallback) {
      return;
    }
    if (!primary.disabled) {
      primary.focus({ preventScroll: true });
      return;
    }
    if (!fallback.disabled) {
      fallback.focus({ preventScroll: true });
    }
  }

  async function loadEventsPage(options) {
    var silent = Boolean(options && options.silent);

    if (silent && state.pageLoading) {
      return;
    }

    cancelAutoRefresh();

    var targetPage = Math.max(1, Math.floor(state.page));
    var targetType = state.selectedType;
    var targetSize = state.pageSize;
    var snapshotTotalAtStart = state.lastSnapshotTotal;
    var seq = state.pageRequestSeq + 1;

    state.pageRequestSeq = seq;
    state.pageLoading = true;
    state.userLoading = !silent;
    state.pageRetryTarget = null;

    if (silent) {
      el.eventsPanel.setAttribute('aria-busy', 'true');
      hideInlineError();
      updateNewDataHint();
    } else {
      showPageLoading();
    }

    var url = '/api/events?limit=' + targetSize +
      '&offset=' + ((targetPage - 1) * targetSize);
    if (targetType) {
      url += '&type=' + encodeURIComponent(targetType);
    }

    try {
      var payload = await requestJSON(url, {
        headers: { 'Accept': 'application/json' }
      });

      if (seq !== state.pageRequestSeq) {
        return;
      }
      if (!isRecord(payload) || !Array.isArray(payload.events)) {
        throw invalidPayload('events');
      }

      var events = normalizeEvents(payload.events);
      var total = Number(payload.total);
      if (!Number.isFinite(total) || total < 0) {
        total = events.length;
      }
      total = Math.max(Math.floor(total), events.length);

      var pages = pageCount(total, targetSize);
      if (targetPage > pages) {
        if (state.page === targetPage &&
            state.selectedType === targetType &&
            state.pageSize === targetSize) {
          state.page = pages;
          state.pageLoading = false;
          state.userLoading = false;
          loadEventsPage(options);
        }
        return;
      }

      state.pageEvents = events;
      state.pageTotal = total;
      state.page = targetPage;
      state.pageLoading = false;
      state.userLoading = false;
      state.pageLoaded = true;
      state.loadedPage = targetPage;
      state.loadedType = targetType;
      state.loadedSize = targetSize;

      if (targetPage === 1) {
        if (targetType) {
          state.seenGlobalTotal = snapshotTotalAtStart !== null
            ? snapshotTotalAtStart
            : state.total;
        } else {
          state.seenGlobalTotal = total;
        }
      }
      // 翻到旧页时不推进基线,否则会误把“有新数据”收起来。

      renderEventsPage({ animate: silent });

      if (!silent && (state.pageFocus === 'next' || state.pageFocus === 'prev')) {
        el.eventsPanel.scrollIntoView({ block: 'start' });
        if (narrowScreen.matches) {
          state.pageFocus = 'title';
        }
      }
      restorePageFocus();

      if (state.page === 1 &&
          !state.selectedType &&
          hasUnseenEvents()) {
        scheduleEventsRefresh();
      }
    } catch (error) {
      if (seq !== state.pageRequestSeq) {
        return;
      }

      state.pageLoading = false;
      state.userLoading = false;
      state.pageRetryTarget = {
        page: targetPage,
        type: targetType,
        size: targetSize
      };

      var message = pageFailureMessage(error, targetType, state.pageLoaded);

      if (silent) {
        el.eventsPanel.setAttribute('aria-busy', 'false');
        showInlineError(message);
        updateNewDataHint();
        return;
      }

      if (state.pageLoaded) {
        state.page = state.loadedPage;
        state.selectedType = state.loadedType;
        state.pageSize = state.loadedSize;
        el.pageSize.value = String(state.pageSize);
        syncFilters();
        renderEventsPage({ animate: false });
        showInlineError(message);
        updateNewDataHint();
        restorePageFocus();
      } else {
        showFirstLoadError();
        showInlineError(message);
        restorePageFocus();
      }
    }
  }

  function pageFailureMessage(error, type, hasContent) {
    if (error && error.timeout) {
      return hasContent
        ? '加载超时，已保留之前的内容。'
        : '加载超时，请重试。';
    }
    if (error && error.network) {
      return hasContent
        ? '连不上服务，已保留之前的内容。'
        : '连不上服务，请检查连接后重试。';
    }
    if (error && error.status >= 500) {
      return hasContent
        ? '服务暂时出了问题，已保留之前的内容。'
        : '服务暂时出了问题，请稍后重试。';
    }
    if (!hasContent) {
      return '没能加载事件，请检查连接后重试。';
    }
    if (type) {
      return '没能加载这个筛选，已保留之前的内容。';
    }
    return '没能加载这一页，已保留之前的内容。';
  }

  function showPageLoading() {
    el.eventsSkeleton.hidden = false;
    el.eventTable.hidden = true;
    el.eventsEmpty.hidden = true;
    el.eventsError.hidden = true;
    el.eventsPanel.setAttribute('aria-busy', 'true');
    el.eventsPrev.disabled = true;
    el.eventsNext.disabled = true;
    el.pageSize.disabled = true;
    setFilterDisabled(true);
    el.eventsPagination.hidden = !state.pageLoaded;
    el.eventsNewData.hidden = true;
    el.eventsStatus.textContent = '';
    hideInlineError();

    if (!state.pageLoaded) {
      el.eventsCount.textContent = '正在加载…';
      el.eventsPageInfo.hidden = true;
    }
  }

  function showFirstLoadError() {
    el.eventsSkeleton.hidden = true;
    el.eventTable.hidden = true;
    el.eventsEmpty.hidden = true;
    el.eventsError.hidden = false;
    el.eventsErrorText.textContent = '请检查连接后重试。';
    el.eventsPagination.hidden = true;
    el.eventsNewData.hidden = true;
    el.eventsStatus.textContent = '';
    el.eventsPanel.setAttribute('aria-busy', 'false');
    el.eventsCount.textContent = '加载失败';
    el.eventsPageInfo.hidden = true;
    el.pageSize.disabled = false;
    setFilterDisabled(false);
  }

  function hideInlineError() {
    el.eventsInlineError.hidden = true;
    el.eventsInlineErrorText.textContent = '';
  }

  function showInlineError(message) {
    el.eventsInlineErrorText.textContent = message;
    el.eventsInlineError.hidden = false;
  }

  function renderEventsPage(options) {
    if (!state.pageLoaded) {
      return;
    }

    var animate = Boolean(options && options.animate);

    hideInlineError();
    el.eventsSkeleton.hidden = true;
    el.eventsError.hidden = true;
    el.eventsPanel.setAttribute('aria-busy', 'false');
    el.pageSize.disabled = false;
    setFilterDisabled(false);

    renderEventRows(state.pageEvents, animate);
    updateEventsView(state.pageEvents, state.pageTotal);
    updateEventsMeta();
    updatePagination();
    updateNewDataHint();

    if (!animate) {
      el.tableWrap.scrollTop = 0;
    }
  }

  function renderEventRows(events, animate) {
    var identities = events.map(eventIdentity);
    var key = state.page + '\u0000' + state.selectedType + '\u0000' +
      state.pageSize + '\u0000' + identities.join('\u0000');

    if (key === state.pageRenderKey) {
      return;
    }

    var previousIds = state.renderedIds;
    var canAnimate = animate && state.hasRenderedPage && previousIds.size > 0;
    var fragment = document.createDocumentFragment();

    events.forEach(function (event, index) {
      var isNew = canAnimate && !previousIds.has(identities[index]);
      fragment.appendChild(createEventRow(event, isNew));
    });

    el.eventRows.textContent = '';
    el.eventRows.appendChild(fragment);
    state.pageRenderKey = key;
    state.renderedIds = new Set(identities);
    state.hasRenderedPage = true;
  }

  function updateEventsView(events, total) {
    if (total === 0) {
      el.eventTable.hidden = true;
      el.eventsError.hidden = true;
      el.eventsEmpty.hidden = false;

      if (state.selectedType) {
        el.eventsEmptyTitle.textContent = '这个类型下暂无事件';
        el.eventsEmptyText.textContent = '切回「全部」查看所有事件。';
      } else {
        el.eventsEmptyTitle.textContent = '还没有事件';
        el.eventsEmptyText.textContent = '到概览页用快速记录写下第一条，或在别处发布事件。';
      }
      return;
    }

    if (events.length === 0) {
      el.eventTable.hidden = true;
      el.eventsError.hidden = true;
      el.eventsEmpty.hidden = false;
      el.eventsEmptyTitle.textContent = '这一页没有事件';
      el.eventsEmptyText.textContent = '返回上一页或回到第 1 页。';
      return;
    }

    el.eventsEmpty.hidden = true;
    el.eventsError.hidden = true;
    el.eventTable.hidden = false;
  }

  function updateEventsMeta() {
    if (!state.pageLoaded) {
      el.eventsPageInfo.hidden = true;
      return;
    }

    var pages = pageCount(state.pageTotal, state.pageSize);
    el.eventsCount.textContent = '共 ' + formatNumber(state.pageTotal) + ' 条';
    el.eventsPageInfo.textContent = '第 ' + state.page + ' / ' + pages + ' 页';
    el.eventsPageInfo.hidden = false;
  }

  function updatePagination() {
    if (!state.pageLoaded) {
      el.eventsPagination.hidden = true;
      return;
    }

    var pages = pageCount(state.pageTotal, state.pageSize);
    el.eventsPagination.hidden = false;
    el.paginationTotal.textContent = '共 ' + formatNumber(state.pageTotal) + ' 条';
    el.paginationPageValue.textContent = state.page + ' / ' + pages;
    el.paginationPage.setAttribute('aria-label', '第 ' + state.page + ' / ' + pages + ' 页');
    el.eventsPrev.disabled = state.userLoading || state.page <= 1;
    el.eventsNext.disabled = state.userLoading || state.page >= pages;
  }

  function createEventRow(event, isNew) {
    var row = document.createElement('tr');
    if (isNew) {
      row.className = 'is-new';
    }

    var rawTypes = eventTypes(event);
    var typeCell = document.createElement('td');
    typeCell.className = 'cell-type';
    var chips = document.createElement('div');
    chips.className = 'type-chips';
    if (rawTypes.length === 0) {
      var emptyChip = document.createElement('span');
      emptyChip.className = 'type-chip';
      emptyChip.textContent = '—';
      chips.appendChild(emptyChip);
    } else {
      rawTypes.forEach(function (rawType) {
        var chip = document.createElement('span');
        chip.className = 'type-chip';
        chip.textContent = rawType;
        chip.setAttribute('title', rawType);
        chips.appendChild(chip);
      });
    }
    typeCell.appendChild(chips);
    row.appendChild(typeCell);

    var source = event && typeof event.source === 'string' && event.source
      ? event.source
      : '未知';
    var sourceCell = document.createElement('td');
    sourceCell.className = 'cell-source';
    var sourceValue = document.createElement('span');
    sourceValue.className = 'source-value';
    sourceValue.textContent = source;
    sourceValue.setAttribute('title', source);
    sourceCell.appendChild(sourceValue);
    row.appendChild(sourceCell);

    var content = contentFor(event);
    var contentCell = document.createElement('td');
    contentCell.className = 'cell-content';
    var contentText = document.createElement('span');
    contentText.className = content.isJSON ? 'content-text is-json' : 'content-text';
    contentText.textContent = content.value;
    contentText.setAttribute('title', content.value);
    contentCell.appendChild(contentText);
    row.appendChild(contentCell);

    var rawTime = event && typeof event.time === 'string' ? event.time : '';
    var full = fullTime(rawTime);
    var timeCell = document.createElement('td');
    timeCell.className = 'cell-time';
    var time = document.createElement('time');
    if (rawTime) {
      time.setAttribute('datetime', rawTime);
    }
    time.textContent = tableTime(rawTime);
    time.setAttribute('title', full);
    time.setAttribute('aria-label', full);
    timeCell.appendChild(time);
    row.appendChild(timeCell);

    return row;
  }

  function contentFor(event) {
    var data = event && event.data;
    if (isRecord(data) && typeof data.text === 'string' && data.text.trim() !== '') {
      return { value: data.text, isJSON: false };
    }
    return { value: compactJSON(data), isJSON: true };
  }

  /* ---- 筛选 ---- */

  function syncFilters() {
    var counts = new Map();
    state.typeCounts.forEach(function (entry) {
      if (!entry || !entry.type || counts.has(entry.type)) {
        return;
      }
      counts.set(entry.type, entry.count);
    });

    var types = Array.from(counts.keys());

    // 选中的类型可能因为事件被归档而暂时不在统计里，
    // 仍然保留它的 chip，避免筛选被静默清掉。
    if (state.selectedType && !counts.has(state.selectedType)) {
      types.push(state.selectedType);
    }

    if (types.length === 0) {
      el.filters.hidden = true;
      el.filters.textContent = '';
      state.filterSignature = '';
      return;
    }

    el.filters.hidden = false;
    var signature = types.join('\u0000');

    if (state.filterSignature === signature) {
      updateFilterTitles(counts);
      updateFilterPressed();
      return;
    }

    var hadFocus = el.filters.contains(document.activeElement);
    el.filters.textContent = '';
    el.filters.appendChild(createFilterChip('', '全部'));
    types.forEach(function (type) {
      el.filters.appendChild(createFilterChip(type, type, counts.get(type)));
    });
    state.filterSignature = signature;
    updateFilterPressed();
    setFilterDisabled(state.userLoading);

    if (hadFocus) {
      var active = el.filters.querySelector('[aria-pressed="true"]');
      if (active) {
        active.focus();
      }
    }
  }

  function updateFilterTitles(counts) {
    var chips = el.filters.querySelectorAll('.filter-chip');
    for (var i = 0; i < chips.length; i += 1) {
      var type = chips[i].getAttribute('data-type') || '';
      if (!type) {
        continue;
      }
      chips[i].setAttribute('title', filterChipTitle(type, counts.get(type)));
    }
  }

  function filterChipTitle(type, count) {
    if (typeof count === 'number' && Number.isFinite(count)) {
      return type + ' · ' + formatNumber(count) + ' 条';
    }
    return type;
  }

  function createFilterChip(type, label, count) {
    var chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'filter-chip';
    chip.setAttribute('data-type', type);
    chip.setAttribute('aria-pressed', 'false');
    chip.textContent = label;
    if (type) {
      chip.setAttribute('title', filterChipTitle(type, count));
    }
    return chip;
  }

  /* ---- 消费者 ---- */

  function renderConsumers() {
    el.consumersSkeleton.hidden = true;
    el.consumersPanel.setAttribute('aria-busy', 'false');
    updateConsumersCount();

    if (state.consumers.length === 0) {
      el.consumerList.textContent = '';
      el.consumerList.hidden = true;
      el.consumersEmpty.hidden = false;
      el.consumersEmptyTitle.textContent = '还没有消费者';
      el.consumersEmptyText.textContent = '注册消费者后，这里会显示游标和投递进度。';
      state.consumerKey = '';
      return;
    }

    el.consumersEmpty.hidden = true;
    el.consumerList.hidden = false;

    var key = state.consumers.map(function (consumer) {
      return [
        consumer.name,
        Array.isArray(consumer.types) ? consumer.types.join(',') : '',
        consumer.cursor,
        consumer.pending,
        consumer.delivered,
        consumer.failed,
        consumer.last_at
      ].join('|');
    }).join('\u0000');

    if (key === state.consumerKey) {
      return;
    }
    state.consumerKey = key;

    var fragment = document.createDocumentFragment();
    state.consumers.forEach(function (consumer) {
      fragment.appendChild(createConsumerRow(consumer));
    });

    el.consumerList.textContent = '';
    el.consumerList.appendChild(fragment);
  }

  function createConsumerRow(consumer) {
    var row = document.createElement('div');
    row.className = 'consumer-row';

    var head = document.createElement('div');
    head.className = 'consumer-head';
    var name = document.createElement('p');
    name.className = 'consumer-name';
    var rawName = consumer && typeof consumer.name === 'string' && consumer.name
      ? consumer.name
      : '未命名消费者';
    name.textContent = rawName;
    name.setAttribute('title', rawName);
    head.appendChild(name);

    var pendingValue = toCount(consumer.pending);
    if (pendingValue === null) {
      pendingValue = 0;
    }
    var lastAt = consumer && typeof consumer.last_at === 'string'
      ? consumer.last_at
      : '';
    var hasLast = !Number.isNaN(parseTimeValue(lastAt));
    var status = consumerStatusFor(pendingValue, lastAt);
    var badge = document.createElement('span');
    badge.className = 'consumer-status';
    badge.setAttribute('data-state', status.state);
    badge.setAttribute('data-pending', String(pendingValue));
    if (hasLast) {
      badge.setAttribute('data-at', lastAt);
    }
    badge.setAttribute('title', status.title);
    var badgeLabel = document.createElement('span');
    badgeLabel.className = 'consumer-status-label';
    badgeLabel.textContent = status.label;
    badge.appendChild(badgeLabel);
    head.appendChild(badge);
    row.appendChild(head);

    if (Array.isArray(consumer.types) && consumer.types.length > 0) {
      var types = document.createElement('div');
      types.className = 'consumer-types';
      consumer.types.forEach(function (type) {
        var raw = String(type);
        var tag = document.createElement('span');
        tag.className = 'type-tag';
        tag.textContent = raw;
        tag.setAttribute('title', raw);
        types.appendChild(tag);
      });
      row.appendChild(types);
    }

    var last = document.createElement('p');
    last.className = 'consumer-last';
    if (hasLast) {
      var lastPrefix = document.createElement('span');
      lastPrefix.textContent = '最后处理';
      var lastTime = document.createElement('time');
      lastTime.className = 'consumer-last-time rel-time';
      lastTime.setAttribute('datetime', lastAt);
      lastTime.setAttribute('data-at', lastAt);
      lastTime.textContent = relativeTime(lastAt);
      last.appendChild(lastPrefix);
      last.appendChild(lastTime);
    } else {
      last.classList.add('is-missing');
      last.textContent = '尚未处理';
    }
    row.appendChild(last);

    var stats = document.createElement('dl');
    stats.className = 'consumer-stats';
    appendMetric(stats, '游标', formatNumber(toCount(consumer.cursor)), false);
    appendMetric(stats, '待处理', formatNumber(toCount(consumer.pending)), false);
    appendMetric(stats, '已投递', formatNumber(toCount(consumer.delivered)), false);

    var failed = toCount(consumer.failed);
    appendMetric(stats, '失败', formatNumber(failed), failed !== null && failed > 0);
    row.appendChild(stats);

    return row;
  }

  function consumerStatusFor(pending, lastAt) {
    if (pending <= 0) {
      return {
        state: 'caught-up',
        label: '已追平',
        title: '待处理 0，已跟上最新事件'
      };
    }
    if (isRecent(lastAt, CONSUMER_STALE_MS)) {
      return {
        state: 'processing',
        label: '处理中',
        title: '有积压，最近 5 分钟内有处理'
      };
    }
    return {
      state: 'backlog',
      label: '积压',
      title: '有积压，超过 5 分钟没有处理'
    };
  }

  function appendMetric(container, label, value, negative) {
    var group = document.createElement('div');
    var term = document.createElement('dt');
    term.textContent = label;
    var detail = document.createElement('dd');
    detail.textContent = value;
    if (negative) {
      detail.className = 'is-negative';
    }
    group.appendChild(term);
    group.appendChild(detail);
    container.appendChild(group);
  }

  function updateConsumersCount() {
    if (!state.hasSnapshot) {
      return;
    }
    el.consumersCount.textContent = state.consumers.length === 0
      ? '尚未注册'
      : formatNumber(state.consumers.length) + ' 个已注册';
  }

  /* ---- 失败记录页 ---- */

  function handleFailureSnapshot(previousFailureTotal) {
    if (previousFailureTotal === null) {
      updateFailureNewDataHint();
      return;
    }

    if (state.view !== 'failures' || !state.failurePageLoaded) {
      return;
    }

    if (state.failureTotal <= previousFailureTotal) {
      updateFailureNewDataHint();
      return;
    }

    if (!state.failurePageLoading && state.failurePage === 1) {
      scheduleFailuresRefresh();
    } else {
      updateFailureNewDataHint();
    }
  }

  function hasUnseenFailures() {
    return state.failureSeenTotal !== null &&
      state.failureTotal > state.failureSeenTotal;
  }

  function ensureFailuresView() {
    if (!state.failurePageLoaded) {
      if (!state.failurePageLoading) {
        loadFailuresPage();
      }
      return;
    }

    if (!state.failurePageLoading &&
        state.failurePage === 1 &&
        hasUnseenFailures()) {
      scheduleFailuresRefresh(0);
    }

    updateFailureNewDataHint();
  }

  function scheduleFailuresRefresh(delay) {
    if (state.view !== 'failures' || !state.failurePageLoaded) {
      return;
    }

    cancelFailureAutoRefresh();

    var wait = typeof delay === 'number' ? delay : NEW_DATA_DEBOUNCE_MS;
    state.failureNewDataTimer = window.setTimeout(function () {
      state.failureNewDataTimer = null;

      if (state.view !== 'failures' || state.failurePageLoading || !state.failurePageLoaded) {
        updateFailureNewDataHint();
        return;
      }
      if (state.failurePage !== 1) {
        updateFailureNewDataHint();
        return;
      }
      loadFailuresPage({ silent: true });
    }, wait);

    updateFailureNewDataHint();
  }

  function cancelFailureAutoRefresh() {
    if (state.failureNewDataTimer !== null) {
      window.clearTimeout(state.failureNewDataTimer);
      state.failureNewDataTimer = null;
    }
  }

  function updateFailureNewDataHint() {
    var autoPending = state.failureNewDataTimer !== null &&
      state.failurePage === 1;

    var show = state.view === 'failures' &&
      state.failurePageLoaded &&
      !autoPending &&
      el.failuresInlineError.hidden &&
      hasUnseenFailures();

    el.failuresNewData.hidden = !show;

    var message = show ? '有新数据，可回到第 1 页刷新。' : '';
    if (el.failuresStatus.textContent !== message) {
      el.failuresStatus.textContent = message;
    }
  }

  function goToFailurePage(page) {
    if (!state.failurePageLoaded || state.failureUserLoading) {
      return;
    }

    var pages = pageCount(state.failurePageTotal, FAILURE_PAGE_SIZE);
    var target = Math.min(Math.max(1, Math.floor(page)), pages);
    if (target === state.failurePage) {
      return;
    }

    var active = document.activeElement;
    cancelFailureAutoRefresh();
    cancelFailurePageLoad();

    state.failurePageFocus = active === el.failuresNext
      ? 'next'
      : active === el.failuresPrev ? 'prev' : '';
    state.failurePage = target;
    state.failurePageRetryTarget = null;
    hideFailureInlineError();
    loadFailuresPage();
  }

  function handleFailureNewDataClick() {
    if (!state.failurePageLoaded) {
      return;
    }

    cancelFailureAutoRefresh();
    cancelFailurePageLoad();

    state.failurePage = 1;
    state.failurePageRetryTarget = null;
    state.failurePageFocus = 'title';
    hideFailureInlineError();
    loadFailuresPage();
  }

  function retryFailurePageLoad() {
    var target = state.failurePageRetryTarget;
    if (!target) {
      return;
    }

    cancelFailureAutoRefresh();
    cancelFailurePageLoad();

    state.failurePage = target.page;
    state.failurePageRetryTarget = null;
    state.failurePageFocus = 'title';
    hideFailureInlineError();
    loadFailuresPage();
  }

  function cancelFailurePageLoad() {
    state.failurePageRequestSeq += 1;
    state.failurePageLoading = false;
    state.failureUserLoading = false;
  }

  function restoreFailurePageFocus() {
    var side = state.failurePageFocus;
    state.failurePageFocus = '';
    if (!side) {
      return;
    }

    var active = document.activeElement;

    if (side === 'title') {
      if (active && active !== document.body &&
          active !== el.failuresNewData && active !== el.failuresRetry &&
          active !== el.failuresPrev && active !== el.failuresNext) {
        return;
      }
      el.failuresTitle.focus({ preventScroll: true });
      return;
    }

    var primary = side === 'next' ? el.failuresNext : el.failuresPrev;
    var fallback = side === 'next' ? el.failuresPrev : el.failuresNext;

    if (active && active !== document.body &&
        active !== primary && active !== fallback) {
      return;
    }
    if (!primary.disabled) {
      primary.focus({ preventScroll: true });
      return;
    }
    if (!fallback.disabled) {
      fallback.focus({ preventScroll: true });
    }
  }

  async function loadFailuresPage(options) {
    var silent = Boolean(options && options.silent);

    if (silent && state.failurePageLoading) {
      return;
    }

    cancelFailureAutoRefresh();

    var targetPage = Math.max(1, Math.floor(state.failurePage));
    var seq = state.failurePageRequestSeq + 1;

    state.failurePageRequestSeq = seq;
    state.failurePageLoading = true;
    state.failureUserLoading = !silent;
    state.failurePageRetryTarget = null;

    if (silent) {
      el.failuresPanel.setAttribute('aria-busy', 'true');
      hideFailureInlineError();
      updateFailureNewDataHint();
    } else {
      showFailurePageLoading();
    }

    var url = '/api/failures?limit=' + FAILURE_PAGE_SIZE +
      '&offset=' + ((targetPage - 1) * FAILURE_PAGE_SIZE);

    try {
      var payload = await requestJSON(url, {
        headers: { 'Accept': 'application/json' }
      });

      if (seq !== state.failurePageRequestSeq) {
        return;
      }
      if (!isRecord(payload) || !Array.isArray(payload.failures)) {
        throw invalidPayload('failures');
      }

      var rows = normalizeFailurePage(payload.failures);
      var total = Number(payload.total);
      if (!Number.isFinite(total) || total < 0) {
        total = rows.length;
      }
      total = Math.max(Math.floor(total), rows.length);

      var pages = pageCount(total, FAILURE_PAGE_SIZE);
      if (targetPage > pages) {
        if (state.failurePage === targetPage) {
          state.failurePage = pages;
          state.failurePageLoading = false;
          state.failureUserLoading = false;
          loadFailuresPage(options);
        }
        return;
      }

      state.failurePageRows = rows;
      state.failurePageTotal = total;
      state.failurePage = targetPage;
      state.failurePageLoading = false;
      state.failureUserLoading = false;
      state.failurePageLoaded = true;
      state.failureLoadedPage = targetPage;

      if (targetPage === 1) {
        state.failureSeenTotal = total;
      }
      // 翻到旧页时不推进基线,否则会误把“有新数据”收起来。

      renderFailurePage({ animate: silent });

      if (!silent && (state.failurePageFocus === 'next' || state.failurePageFocus === 'prev')) {
        el.failuresPanel.scrollIntoView({ block: 'start' });
        if (narrowScreen.matches) {
          state.failurePageFocus = 'title';
        }
      }
      restoreFailurePageFocus();

      if (state.failurePage === 1 && hasUnseenFailures()) {
        scheduleFailuresRefresh();
      }
    } catch (error) {
      if (seq !== state.failurePageRequestSeq) {
        return;
      }

      state.failurePageLoading = false;
      state.failureUserLoading = false;
      state.failurePageRetryTarget = { page: targetPage };

      var message = failurePageFailureMessage(error, state.failurePageLoaded);

      if (silent) {
        el.failuresPanel.setAttribute('aria-busy', 'false');
        showFailureInlineError(message);
        updateFailureNewDataHint();
        return;
      }

      if (state.failurePageLoaded) {
        state.failurePage = state.failureLoadedPage;
        renderFailurePage({ animate: false });
        showFailureInlineError(message);
        updateFailureNewDataHint();
        restoreFailurePageFocus();
      } else {
        showFirstFailureLoadError();
        showFailureInlineError(message);
        restoreFailurePageFocus();
      }
    }
  }

  function failurePageFailureMessage(error, hasContent) {
    if (error && error.timeout) {
      return hasContent
        ? '加载超时，已保留之前的内容。'
        : '加载超时，请重试。';
    }
    if (error && error.network) {
      return hasContent
        ? '连不上服务，已保留之前的内容。'
        : '连不上服务，请检查连接后重试。';
    }
    if (error && error.status >= 500) {
      return hasContent
        ? '服务暂时出了问题，已保留之前的内容。'
        : '服务暂时出了问题，请稍后重试。';
    }
    if (!hasContent) {
      return '没能加载失败记录，请检查连接后重试。';
    }
    return '没能加载这一页，已保留之前的内容。';
  }

  function showFailurePageLoading() {
    el.failuresSkeleton.hidden = false;
    el.failuresTable.hidden = true;
    el.failuresEmpty.hidden = true;
    el.failuresError.hidden = true;
    el.failuresPanel.setAttribute('aria-busy', 'true');
    el.failuresPrev.disabled = true;
    el.failuresNext.disabled = true;
    el.failuresPagination.hidden = !state.failurePageLoaded;
    el.failuresNewData.hidden = true;
    el.failuresStatus.textContent = '';
    hideFailureInlineError();

    if (!state.failurePageLoaded) {
      el.failuresCount.textContent = '正在加载…';
      el.failuresPageInfo.hidden = true;
    }
  }

  function showFirstFailureLoadError() {
    el.failuresSkeleton.hidden = true;
    el.failuresTable.hidden = true;
    el.failuresEmpty.hidden = true;
    el.failuresError.hidden = false;
    el.failuresErrorText.textContent = '请检查连接后重试。';
    el.failuresPagination.hidden = true;
    el.failuresNewData.hidden = true;
    el.failuresStatus.textContent = '';
    el.failuresPanel.setAttribute('aria-busy', 'false');
    el.failuresCount.textContent = '加载失败';
    el.failuresPageInfo.hidden = true;
  }

  function hideFailureInlineError() {
    el.failuresInlineError.hidden = true;
    el.failuresInlineErrorText.textContent = '';
  }

  function showFailureInlineError(message) {
    el.failuresInlineErrorText.textContent = message;
    el.failuresInlineError.hidden = false;
  }

  function renderFailurePage(options) {
    if (!state.failurePageLoaded) {
      return;
    }

    var animate = Boolean(options && options.animate);

    hideFailureInlineError();
    el.failuresSkeleton.hidden = true;
    el.failuresError.hidden = true;
    el.failuresPanel.setAttribute('aria-busy', 'false');

    renderFailureRows(state.failurePageRows);
    updateFailureView(state.failurePageRows, state.failurePageTotal);
    updateFailureMeta();
    updateFailurePagination();
    updateFailureNewDataHint();

    if (!animate) {
      el.failuresTableWrap.scrollTop = 0;
    }
  }

  function renderFailureRows(rows) {
    var key = state.failurePage + '\u0000' + rows.map(function (failure) {
      return JSON.stringify([
        failure && failure.consumer,
        failure && failure.event_id,
        failure && failure.error,
        failure && failure.at
      ]);
    }).join('\u0000');

    if (key === state.failurePageRenderKey) {
      return;
    }
    state.failurePageRenderKey = key;

    var fragment = document.createDocumentFragment();
    rows.forEach(function (failure) {
      fragment.appendChild(createFailureTableRow(failure));
    });

    el.failureRows.textContent = '';
    el.failureRows.appendChild(fragment);
  }

  function createFailureTableRow(failure) {
    var row = document.createElement('tr');

    var consumer = failure && typeof failure.consumer === 'string' && failure.consumer
      ? failure.consumer
      : '未知消费者';
    var consumerCell = document.createElement('td');
    consumerCell.className = 'cell-f-consumer';
    var consumerName = document.createElement('span');
    consumerName.className = 'failure-consumer-name';
    consumerName.textContent = consumer;
    consumerName.setAttribute('title', consumer);
    consumerCell.appendChild(consumerName);
    row.appendChild(consumerCell);

    var eventID = failure && typeof failure.event_id === 'string' ? failure.event_id : '';
    var eventCell = document.createElement('td');
    eventCell.className = 'cell-f-event';
    var eventValue = document.createElement('span');
    eventValue.className = 'failure-event-id';
    eventValue.textContent = eventID || '—';
    if (eventID) {
      eventValue.setAttribute('title', eventID);
    }
    eventCell.appendChild(eventValue);
    row.appendChild(eventCell);

    var source = failure && typeof failure.source === 'string' && failure.source
      ? failure.source
      : '未知';
    var sourceCell = document.createElement('td');
    sourceCell.className = 'cell-f-source';
    var sourceValue = document.createElement('span');
    sourceValue.className = 'failure-source-value';
    sourceValue.textContent = source;
    sourceValue.setAttribute('title', source);
    sourceCell.appendChild(sourceValue);
    row.appendChild(sourceCell);

    var message = failure && typeof failure.error === 'string' && failure.error
      ? failure.error
      : '未知错误';
    var errorCell = document.createElement('td');
    errorCell.className = 'cell-f-error';
    var errorText = document.createElement('span');
    errorText.className = 'failure-error-text';
    errorText.textContent = message;
    errorText.setAttribute('title', message);
    errorCell.appendChild(errorText);
    row.appendChild(errorCell);

    var rawTime = failure && typeof failure.at === 'string' ? failure.at : '';
    var full = fullTime(rawTime);
    var timeCell = document.createElement('td');
    timeCell.className = 'cell-time';
    var time = document.createElement('time');
    if (rawTime) {
      time.setAttribute('datetime', rawTime);
    }
    time.textContent = tableTime(rawTime);
    time.setAttribute('title', full);
    time.setAttribute('aria-label', full);
    timeCell.appendChild(time);
    row.appendChild(timeCell);

    return row;
  }

  function updateFailureView(rows, total) {
    if (total === 0) {
      el.failuresTable.hidden = true;
      el.failuresError.hidden = true;
      el.failuresEmpty.hidden = false;
      el.failuresEmptyTitle.textContent = '没有失败记录';
      el.failuresEmptyText.textContent = '被放弃的事件会记录在这里。';
      return;
    }

    if (rows.length === 0) {
      el.failuresTable.hidden = true;
      el.failuresError.hidden = true;
      el.failuresEmpty.hidden = false;
      el.failuresEmptyTitle.textContent = '这一页没有记录';
      el.failuresEmptyText.textContent = '返回上一页或回到第 1 页。';
      return;
    }

    el.failuresEmpty.hidden = true;
    el.failuresError.hidden = true;
    el.failuresTable.hidden = false;
  }

  function updateFailureMeta() {
    if (!state.failurePageLoaded) {
      el.failuresPageInfo.hidden = true;
      return;
    }

    var pages = pageCount(state.failurePageTotal, FAILURE_PAGE_SIZE);
    el.failuresCount.textContent = '共 ' + formatNumber(state.failurePageTotal) + ' 条';
    el.failuresPageInfo.textContent = '第 ' + state.failurePage + ' / ' + pages + ' 页';
    el.failuresPageInfo.hidden = false;
  }

  function updateFailurePagination() {
    if (!state.failurePageLoaded) {
      el.failuresPagination.hidden = true;
      return;
    }

    var pages = pageCount(state.failurePageTotal, FAILURE_PAGE_SIZE);
    el.failuresPagination.hidden = false;
    el.failuresPaginationTotal.textContent = '共 ' + formatNumber(state.failurePageTotal) + ' 条';
    el.failuresPageValue.textContent = state.failurePage + ' / ' + pages;
    el.failuresPaginationPage.setAttribute(
      'aria-label',
      '第 ' + state.failurePage + ' / ' + pages + ' 页'
    );
    el.failuresPrev.disabled = state.failureUserLoading || state.failurePage <= 1;
    el.failuresNext.disabled = state.failureUserLoading || state.failurePage >= pages;
  }

  /* ---- 侧栏加载状态 ---- */

  function showSideLoading() {
    el.consumersSkeleton.hidden = false;
    el.consumerList.hidden = true;
    el.consumersEmpty.hidden = true;
    el.consumersPanel.setAttribute('aria-busy', 'true');
    el.consumersCount.textContent = '正在连接…';

    el.producersSkeleton.hidden = false;
    el.producerList.hidden = true;
    el.producersEmpty.hidden = true;
    el.producersMore.hidden = true;
    el.producersPanel.setAttribute('aria-busy', 'true');
    el.producersCount.textContent = '正在连接…';
  }

  function showSideWaiting() {
    el.consumersSkeleton.hidden = true;
    el.consumerList.hidden = true;
    el.consumersEmpty.hidden = false;
    el.consumersEmptyTitle.textContent = '等待连接';
    el.consumersEmptyText.textContent = '连接恢复后会显示消费者进度。';
    el.consumersPanel.setAttribute('aria-busy', 'false');
    el.consumersCount.textContent = '连接中断';

    el.producersSkeleton.hidden = true;
    el.producerList.hidden = true;
    el.producersMore.hidden = true;
    el.producersEmpty.hidden = false;
    el.producersEmptyTitle.textContent = '等待连接';
    el.producersEmptyText.textContent = '连接恢复后会显示输入来源。';
    el.producersPanel.setAttribute('aria-busy', 'false');
    el.producersCount.textContent = '连接中断';
  }

  /* ---- 连接状态 ---- */

  function setConnection(mode) {
    state.connection = mode;
    var label = CONNECTION_LABELS[mode] || CONNECTION_LABELS.connecting;

    if (el.connection.dataset.state !== mode) {
      el.connection.dataset.state = mode;
    }
    if (el.sidebarStatus.dataset.state !== mode) {
      el.sidebarStatus.dataset.state = mode;
    }
    if (el.connectionLabel.textContent !== label) {
      el.connectionLabel.textContent = label;
    }
    if (el.sidebarConnectionLabel.textContent !== label) {
      el.sidebarConnectionLabel.textContent = label;
    }
  }

  function updateStreamNote() {
    var visible = false;
    var message = '';

    if (state.unsupported) {
      visible = true;
      message = '当前浏览器不支持实时推送，概览数据不会自动更新。';
    } else if (!state.hasSnapshot && state.streamFailed) {
      visible = true;
      message = '暂时连不上实时推送，正在自动重连。';
    } else if (state.hasSnapshot && state.connection !== 'online') {
      visible = true;
      message = '连接中断，正在自动重连。概览保留最后一次收到的数据。';
    }

    if (el.streamNoteText.textContent !== message) {
      el.streamNoteText.textContent = message;
    }
    el.streamNote.hidden = !visible;
  }

  /* ---- 请求 ---- */

  async function requestJSON(url, options) {
    var controller = new AbortController();
    var timeoutId = window.setTimeout(function () {
      controller.abort();
    }, REQUEST_TIMEOUT_MS);

    var request = Object.assign({}, options || {});
    request.signal = controller.signal;
    request.cache = 'no-store';
    if (!request.headers) {
      request.headers = { 'Accept': 'application/json' };
    }

    try {
      var response;
      try {
        response = await fetch(url, request);
      } catch (error) {
        if (error && error.name === 'AbortError') {
          var timeoutError = new Error('request timed out');
          timeoutError.timeout = true;
          throw timeoutError;
        }
        var networkError = new Error('network request failed');
        networkError.network = true;
        throw networkError;
      }

      var raw = await response.text();
      var data = null;
      if (raw) {
        try {
          data = JSON.parse(raw);
        } catch (parseError) {
          data = null;
        }
      }

      if (!response.ok) {
        var apiError = new Error('request failed');
        apiError.status = response.status;
        if (data && typeof data.error === 'string' && data.error) {
          apiError.detail = data.error;
        } else if (raw) {
          apiError.detail = raw.slice(0, 200);
        }
        throw apiError;
      }

      if (!raw) {
        return null;
      }
      if (data === null) {
        throw invalidPayload('json');
      }
      return data;
    } finally {
      window.clearTimeout(timeoutId);
    }
  }

  function invalidPayload(what) {
    var error = new Error('invalid payload');
    error.detail = 'invalid payload: ' + what;
    return error;
  }

  /* ---- 数据整理 ---- */

  function normalizeEvents(raw) {
    if (!Array.isArray(raw)) {
      return [];
    }

    // 服务端按到达顺序(seq,旧→新)切页并返回,展示需要新→旧。
    // 这里只反转数组,不按 time 排序,避免时钟不准的事件跨页错位。
    var rows = [];
    raw.forEach(function (value) {
      if (isRecord(value)) {
        rows.push(value);
      }
    });
    return rows.reverse();
  }

  function normalizeTypeCounts(raw) {
    if (!Array.isArray(raw)) {
      return [];
    }

    var counts = [];
    var seen = new Set();
    raw.forEach(function (value) {
      if (!isRecord(value) || typeof value.type !== 'string') {
        return;
      }
      var type = value.type.trim();
      if (!type || seen.has(type)) {
        return;
      }
      var count = Number(value.count);
      if (!Number.isFinite(count) || count < 0) {
        return;
      }
      seen.add(type);
      counts.push({ type: type, count: Math.floor(count) });
    });
    return counts;
  }

  function normalizeFailurePage(raw) {
    if (!Array.isArray(raw)) {
      return [];
    }

    // 服务端按发生顺序(旧→新)切页并返回,展示需要新→旧。
    // 与事件页一致,只反转数组,不按时间重排,避免跨页错位。
    var rows = [];
    raw.forEach(function (value) {
      if (isRecord(value)) {
        rows.push(value);
      }
    });
    return rows.reverse();
  }

  /* ---- 小工具 ---- */

  function isRecord(value) {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
  }

  function eventIdentity(event) {
    if (event && event.id !== undefined && event.id !== null && String(event.id) !== '') {
      return String(event.id);
    }
    var time = event && event.time ? String(event.time) : '';
    var type = eventTypes(event).join(',');
    var source = event && event.source ? String(event.source) : '';
    var data = '';
    try {
      data = JSON.stringify(event && event.data);
    } catch (error) {
      data = '';
    }
    return 'fallback:' + time + '|' + type + '|' + source + '|' + data;
  }

  function eventTypes(event) {
    var types = [];
    if (!event || !Array.isArray(event.types)) {
      return types;
    }
    event.types.forEach(function (value) {
      if (typeof value !== 'string') {
        return;
      }
      var type = value.trim();
      if (type && types.indexOf(type) === -1) {
        types.push(type);
      }
    });
    return types;
  }

  function compactJSON(value) {
    if (value === undefined) {
      return '{}';
    }
    try {
      var text = JSON.stringify(value);
      return typeof text === 'string' ? text : String(value);
    } catch (error) {
      return String(value);
    }
  }

  function tableTime(raw) {
    var time = Date.parse(raw);
    if (Number.isNaN(time)) {
      return '—';
    }

    var date = new Date(time);
    var now = new Date();
    var clock = pad2(date.getHours()) + ':' + pad2(date.getMinutes());

    if (date.getFullYear() === now.getFullYear() &&
        date.getMonth() === now.getMonth() &&
        date.getDate() === now.getDate()) {
      return clock;
    }

    if (date.getFullYear() === now.getFullYear()) {
      return pad2(date.getMonth() + 1) + '/' + pad2(date.getDate()) + ' ' + clock;
    }

    return date.getFullYear() + '/' +
      pad2(date.getMonth() + 1) + '/' + pad2(date.getDate());
  }

  function fullTime(raw) {
    var time = Date.parse(raw);
    if (Number.isNaN(time)) {
      return '时间未知';
    }
    var date = new Date(time);
    return date.getFullYear() + ' 年 ' +
      (date.getMonth() + 1) + ' 月 ' +
      date.getDate() + ' 日 ' +
      pad2(date.getHours()) + ':' +
      pad2(date.getMinutes()) + ':' +
      pad2(date.getSeconds());
  }

  function pad2(value) {
    return value < 10 ? '0' + value : String(value);
  }

  function toCount(value) {
    var number = Number(value);
    if (!Number.isFinite(number) || number < 0) {
      return null;
    }
    return number;
  }

  function formatNumber(value) {
    if (value === null || value === undefined) {
      return '—';
    }
    var number = Number(value);
    if (!Number.isFinite(number)) {
      return '—';
    }
    return numberFormatter.format(number);
  }

  init();
})();
