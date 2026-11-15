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

  var COLUMN_WIDTH_KEY = 'yi.events.columnWidths';
  var COLUMN_DEFAULTS = { type: 136, source: 118, time: 192 };
  var COLUMN_MIN = { type: 96, source: 88, time: 184 };
  var COLUMN_MAX = { type: 320, source: 280, time: 360 };
  var CONTENT_MIN_WIDTH = 160;
  var SELECT_COL_WIDTH = 44;
  var ACTIONS_COL_WIDTH = 44;
  var MAX_DELETE_BATCH = 1000;

  var REL_TIME_TICK_MS = 30 * 1000;
  var PRODUCER_LIMIT = 6;
  var PRODUCER_ACTIVE_MS = 2 * 60 * 1000;
  var CONSUMER_STALE_MS = 5 * 60 * 1000;
  var STREAM_AUTH_PROBE_MS = 1200;

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

    /* 认证 */
    auth: {
      authenticated: false,
      authEnabled: false,
      loginBusy: false,
      logoutBusy: false,
      generation: 0
    },
    dashboardStarted: false,
    streamSource: null,
    streamAuthProbeTimer: null,
    streamAuthProbeBusy: false,

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
    selectedConsumerNames: new Set(),
    selectedProducerSources: new Set(),
    lastSnapshotTotal: null,

    /* 相对时间 */
    relTimeTimer: null,

    /* 采集 */
    captureBusy: false,
    statusTimer: null,
    statusFadeTimer: null,

    /* 事件页 */
    pageSize: readStoredPageSize(),
    columnWidths: readStoredColumnWidths(),
    columnDrag: null,
    selectedEventIds: new Set(), /* composite source+id keys */
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
    failureNewDataTimer: null,

    /* 删除确认 */
    confirm: null,
    deleteBusy: false,

    /* 事件详情 */
    detail: null,
    detailRowData: new WeakMap(),
    detailBackdropDown: false,
    detailStatusTimer: null
  };

  /* ---- 启动 ---- */

  function init() {
    el.skipLink = document.querySelector('.skip-link');
    el.shell = document.querySelector('.shell');
    el.pageTitle = document.getElementById('page-title');
    el.pageSub = document.getElementById('page-sub');
    el.viewOverview = document.getElementById('view-overview');
    el.viewEvents = document.getElementById('view-events');
    el.viewFailures = document.getElementById('view-failures');

    el.authScreen = document.getElementById('auth-screen');
    el.authChecking = document.getElementById('auth-checking');
    el.authCheckingText = document.getElementById('auth-checking-text');
    el.authFormWrap = document.getElementById('auth-form-wrap');
    el.loginForm = document.getElementById('login-form');
    el.authToken = document.getElementById('auth-token');
    el.authReveal = document.getElementById('auth-reveal');
    el.authError = document.getElementById('auth-error');
    el.authErrorText = document.getElementById('auth-error-text');
    el.authSubmit = document.getElementById('auth-submit');
    el.authSubmitLabel = document.getElementById('auth-submit-label');
    el.logoutButton = document.getElementById('logout-button');
    el.logoutLabel = document.getElementById('logout-label');
    el.mobileLogout = document.getElementById('mobile-logout');
    el.logoutAlert = document.getElementById('logout-alert');
    el.logoutAlertText = document.getElementById('logout-alert-text');
    el.logoutAlertRetry = document.getElementById('logout-alert-retry');
    el.logoutAlertDismiss = document.getElementById('logout-alert-dismiss');

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
    el.eventsSelectAll = document.getElementById('events-select-all');
    el.eventsSelectAllMobile = document.getElementById('events-select-all-mobile');
    el.eventsSelection = document.getElementById('events-selection');
    el.eventsSelectionCount = document.getElementById('events-selection-count');
    el.eventsSelectionClear = document.getElementById('events-selection-clear');
    el.eventsBulkDelete = document.getElementById('events-bulk-delete');
    el.eventCols = {
      type: el.eventTable.querySelector('col[data-column="type"]'),
      source: el.eventTable.querySelector('col[data-column="source"]'),
      time: el.eventTable.querySelector('col[data-column="time"]')
    };
    el.eventResizers = {
      type: el.eventTable.querySelector('.col-resizer[data-column="type"]'),
      source: el.eventTable.querySelector('.col-resizer[data-column="source"]'),
      content: el.eventTable.querySelector('.col-resizer[data-column="content"]'),
      time: el.eventTable.querySelector('.col-resizer[data-column="time"]')
    };

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
    el.consumersTitle = document.getElementById('consumers-title');
    el.consumersCount = document.getElementById('consumers-count');
    el.consumerList = document.getElementById('consumer-list');
    el.consumersSkeleton = document.getElementById('consumers-skeleton');
    el.consumersEmpty = document.getElementById('consumers-empty');
    el.consumersEmptyTitle = document.getElementById('consumers-empty-title');
    el.consumersEmptyText = document.getElementById('consumers-empty-text');
    el.consumersSelectAll = document.getElementById('consumers-select-all');
    el.consumersSelection = document.getElementById('consumers-selection');
    el.consumersSelectionCount = document.getElementById('consumers-selection-count');
    el.consumersSelectionClear = document.getElementById('consumers-selection-clear');
    el.consumersBulkDelete = document.getElementById('consumers-bulk-delete');

    el.producersPanel = document.getElementById('producers');
    el.producersTitle = document.getElementById('producers-title');
    el.producersCount = document.getElementById('producers-count');
    el.producerList = document.getElementById('producer-list');
    el.producersSkeleton = document.getElementById('producers-skeleton');
    el.producersEmpty = document.getElementById('producers-empty');
    el.producersEmptyTitle = document.getElementById('producers-empty-title');
    el.producersEmptyText = document.getElementById('producers-empty-text');
    el.producersMore = document.getElementById('producers-more');
    el.producersSelectAll = document.getElementById('producers-select-all');
    el.producersSelection = document.getElementById('producers-selection');
    el.producersSelectionCount = document.getElementById('producers-selection-count');
    el.producersSelectionClear = document.getElementById('producers-selection-clear');
    el.producersBulkDelete = document.getElementById('producers-bulk-delete');

    el.confirmDialog = document.getElementById('confirm-dialog');
    el.confirmTitle = document.getElementById('confirm-title');
    el.confirmMessage = document.getElementById('confirm-message');
    el.confirmWarning = document.getElementById('confirm-warning');
    el.confirmError = document.getElementById('confirm-error');
    el.confirmErrorText = document.getElementById('confirm-error-text');
    el.confirmCancel = document.getElementById('confirm-cancel');
    el.confirmSubmit = document.getElementById('confirm-submit');
    el.deleteStatus = document.getElementById('delete-status');

    el.eventDetailDialog = document.getElementById('event-detail-dialog');
    el.eventDetailClose = document.getElementById('event-detail-close');
    el.eventDetailBody = document.getElementById('event-detail-body');
    el.eventDetailTypes = document.getElementById('event-detail-types');
    el.eventDetailSource = document.getElementById('event-detail-source');
    el.eventDetailId = document.getElementById('event-detail-id');
    el.eventDetailTime = document.getElementById('event-detail-time');
    el.eventDetailTimeRaw = document.getElementById('event-detail-time-raw');
    el.eventDetailJson = document.getElementById('event-detail-json');
    el.eventDetailCopy = document.getElementById('event-detail-copy');
    el.eventDetailStatus = document.getElementById('event-detail-status');

    el.keyMod.textContent = isMac ? 'Cmd' : 'Ctrl';
    el.pageSize.value = String(state.pageSize);
    applyEventColumnWidths();

    bindEvents();
    syncCaptureSubmit();
    autosize();
    setConnection('connecting');
    updateStreamNote();

    bootstrapAuth();
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

    el.eventsSelectAll.addEventListener('change', handleEventSelectAllChange);
    el.eventsSelectAllMobile.addEventListener('change', handleEventSelectAllChange);
    el.eventsSelectionClear.addEventListener('click', function () {
      clearEventSelection(true);
    });
    el.eventsBulkDelete.addEventListener('click', openSelectedEventsDeleteDialog);
    el.eventRows.addEventListener('change', handleEventRowSelectChange);
    el.eventRows.addEventListener('click', handleEventRowDeleteClick);

    el.consumersSelectAll.addEventListener('change', handleConsumerSelectAllChange);
    el.consumersSelectionClear.addEventListener('click', function () {
      clearConsumerSelection(true);
    });
    el.consumersBulkDelete.addEventListener('click', openSelectedConsumersDeleteDialog);
    el.consumerList.addEventListener('change', handleConsumerSelectChange);
    el.consumerList.addEventListener('click', handleConsumerDeleteClick);

    el.producersSelectAll.addEventListener('change', handleProducerSelectAllChange);
    el.producersSelectionClear.addEventListener('click', function () {
      clearProducerSelection(true);
    });
    el.producersBulkDelete.addEventListener('click', openSelectedProducersDeleteDialog);
    el.producerList.addEventListener('change', handleProducerSelectChange);
    el.producerList.addEventListener('click', handleProducerDeleteClick);
    el.producerList.addEventListener('click', handleProducerHideClick);

    el.eventTable.addEventListener('pointerdown', handleColumnResizeStart);
    el.eventTable.addEventListener('keydown', handleColumnResizeKeydown);
    document.addEventListener('pointermove', handleColumnResizeMove);
    document.addEventListener('pointerup', handleColumnResizeEnd);
    document.addEventListener('pointercancel', handleColumnResizeEnd);

    el.confirmCancel.addEventListener('click', function () {
      closeConfirmDialog();
    });
    el.confirmSubmit.addEventListener('click', performConfirmedDelete);
    el.confirmDialog.addEventListener('cancel', handleConfirmCancel);
    el.confirmDialog.addEventListener('close', handleConfirmClose);

    el.eventRows.addEventListener('dblclick', handleEventRowDoubleClick);
    el.eventDetailClose.addEventListener('click', closeEventDetailDialog);
    el.eventDetailCopy.addEventListener('click', copyEventDetailJson);
    el.eventDetailDialog.addEventListener('pointerdown', handleEventDetailPointerDown);
    el.eventDetailDialog.addEventListener('click', handleEventDetailClick);
    el.eventDetailDialog.addEventListener('close', handleEventDetailClose);

    el.loginForm.addEventListener('submit', function (event) {
      event.preventDefault();
      submitLogin();
    });

    el.authToken.addEventListener('input', handleAuthTokenInput);
    el.authReveal.addEventListener('click', toggleTokenVisibility);
    el.logoutButton.addEventListener('click', function () {
      handleLogout(false);
    });
    el.mobileLogout.addEventListener('click', function () {
      handleLogout(false);
    });
    el.logoutAlertRetry.addEventListener('click', function () {
      handleLogout(true);
    });
    el.logoutAlertDismiss.addEventListener('click', handleLogoutAlertDismiss);
    document.addEventListener('keydown', handleAuthKeydown);

    document.addEventListener('visibilitychange', handleVisibilityChange);
    window.addEventListener('resize', handleWindowResize);
  }

  function handleWindowResize() {
    autosize();
    applyEventColumnWidths();
  }

  /* ---- 认证 ---- */

  async function bootstrapAuth() {
    showAuthChecking();

    var session;
    try {
      session = await fetchSession();
    } catch (error) {
      showLoginOverlay({
        message: sessionFailureMessage(error),
        resetToken: true
      });
      return;
    }

    state.auth.authEnabled = session.authEnabled;

    if (session.authenticated) {
      enterAuthenticatedApp({ initial: true });
      return;
    }

    state.auth.authenticated = false;
    showLoginOverlay({ resetToken: true });
  }

  function showAuthChecking() {
    hideLogoutAlert();
    el.authScreen.hidden = false;
    el.authScreen.removeAttribute('aria-hidden');
    el.authScreen.scrollTop = 0;
    el.authChecking.hidden = false;
    el.authFormWrap.hidden = true;
    el.authCheckingText.textContent = '正在确认登录状态…';
    updateAuthChrome();
    lockApp(true);
    setConnection('connecting');
    document.title = 'Yi · 登录';
  }

  function showLoginOverlay(options) {
    var opts = options || {};
    hideLogoutAlert();
    el.authScreen.hidden = false;
    el.authScreen.removeAttribute('aria-hidden');
    el.authScreen.scrollTop = 0;
    el.authChecking.hidden = true;
    el.authFormWrap.hidden = false;
    setAuthBusy(false);

    if (opts.resetToken) {
      el.authToken.value = '';
      setTokenVisibility(false);
    }

    if (opts.message) {
      showAuthError(opts.message, false);
    } else {
      clearAuthError();
    }

    updateAuthChrome();
    focusAuthToken();
    lockApp(true);
    document.title = 'Yi · 登录';
  }

  function hideAuthScreen() {
    el.authScreen.hidden = true;
    el.authScreen.setAttribute('aria-hidden', 'true');
    el.authFormWrap.hidden = true;
    el.authChecking.hidden = false;
    setAuthBusy(false);
  }

  function enterAuthenticatedApp(options) {
    var initial = Boolean(options && options.initial);
    var hadAuthFocus = el.authScreen.contains(document.activeElement);

    state.auth.authenticated = true;
    state.auth.generation += 1;

    hideAuthScreen();
    lockApp(false);
    updateAuthChrome();
    hideLogoutAlert();
    clearAuthError();
    updateViewChrome(state.view);

    if (!initial && hadAuthFocus) {
      focusMainHeading();
    }

    if (state.dashboardStarted) {
      resumeDashboard();
    } else {
      startDashboard();
    }
  }

  function startDashboard() {
    if (state.dashboardStarted) {
      resumeDashboard();
      return;
    }

    state.dashboardStarted = true;

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

  function resumeDashboard() {
    setConnection('connecting');
    updateStreamNote();
    startRelativeTimeTicker();

    if (typeof window.EventSource === 'function') {
      startStream();
    } else {
      handleUnsupportedStream();
    }

    if (state.view === 'events') {
      state.pageFocus = '';
      hideInlineError();
      loadEventsPage();
    } else if (state.view === 'failures') {
      state.failurePageFocus = '';
      hideFailureInlineError();
      loadFailuresPage();
    }
  }

  function stopDashboard() {
    stopStream();
    stopRelativeTimeTicker();
    cancelAutoRefresh();
    cancelFailureAutoRefresh();
    cancelPageLoad();
    cancelFailurePageLoad();
    clearCaptureStatus();
    closeConfirmDialog({ restoreFocus: false });
    clearAllSelections();
  }

  async function fetchSession() {
    var payload = await requestJSON('/api/session', {
      headers: { 'Accept': 'application/json' },
      authCheck: true
    });

    if (!isRecord(payload) || typeof payload.authenticated !== 'boolean') {
      throw invalidPayload('session');
    }

    return {
      authenticated: payload.authenticated === true,
      authEnabled: payload.auth_enabled === true
    };
  }

  function handleAuthTokenInput() {
    if (!el.authError.hidden) {
      clearAuthError();
    }
  }

  function toggleTokenVisibility() {
    setTokenVisibility(el.authToken.type === 'password');
    if (!state.auth.loginBusy) {
      focusAuthToken();
    }
  }

  function setTokenVisibility(revealed) {
    el.authToken.type = revealed ? 'text' : 'password';
    el.authReveal.setAttribute('aria-pressed', revealed ? 'true' : 'false');
    el.authReveal.setAttribute('aria-label', revealed ? '隐藏令牌' : '显示令牌');
    el.authReveal.setAttribute('title', revealed ? '隐藏令牌' : '显示令牌');
  }

  async function submitLogin() {
    if (state.auth.loginBusy || state.auth.authenticated) {
      return;
    }

    var token = el.authToken.value.trim();
    if (!token) {
      showAuthError('请输入访问令牌。', true);
      focusAuthToken();
      return;
    }

    clearAuthError();
    setAuthBusy(true);

    try {
      await requestJSON('/api/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({ token: token }),
        authCheck: true
      });

      setAuthBusy(false);
      el.authToken.value = '';
      setTokenVisibility(false);
      state.auth.authEnabled = true;
      enterAuthenticatedApp({ initial: false });
    } catch (error) {
      setAuthBusy(false);
      showAuthError(loginFailureMessage(error), Boolean(error && error.status === 401));
      focusAuthToken();
    }
  }

  function setAuthBusy(busy) {
    state.auth.loginBusy = busy;
    el.loginForm.setAttribute('aria-busy', busy ? 'true' : 'false');
    el.authSubmit.disabled = busy;
    el.authSubmit.setAttribute('aria-busy', busy ? 'true' : 'false');
    el.authSubmitLabel.textContent = busy ? '登录中…' : '登录';
    el.authToken.readOnly = busy;
    el.authReveal.disabled = busy;
  }

  function showAuthError(message, markInvalid) {
    el.authErrorText.textContent = message;
    el.authError.hidden = false;
    if (markInvalid) {
      el.authToken.setAttribute('aria-invalid', 'true');
    } else {
      el.authToken.removeAttribute('aria-invalid');
    }
  }

  function clearAuthError() {
    el.authError.hidden = true;
    el.authErrorText.textContent = '';
    el.authToken.removeAttribute('aria-invalid');
  }

  function sessionFailureMessage(error) {
    if (error && error.timeout) {
      return '确认登录状态超时，请检查连接后重试。';
    }
    if (error && error.network) {
      return '无法连接服务，请检查连接后重试。';
    }
    if (error && error.status === 404) {
      return '登录服务不可用，请稍后重试。';
    }
    if (error && error.status >= 500) {
      return '服务暂时不可用，请稍后重试。';
    }
    return '无法确认登录状态，请重试。';
  }

  function loginFailureMessage(error) {
    if (error && error.status === 401) {
      return '令牌不正确，请检查后重试。';
    }
    if (error && error.timeout) {
      return '登录请求超时，请重试。';
    }
    if (error && error.network) {
      return '连不上服务，请检查网络后重试。';
    }
    if (error && error.status === 429) {
      return '尝试次数过多，请稍后再试。';
    }
    if (error && error.status >= 500) {
      return '服务暂时不可用，请稍后重试。';
    }
    return '登录失败，请稍后重试。';
  }

  function handleAuthKeydown(event) {
    if (el.authScreen.hidden) {
      return;
    }

    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      return;
    }

    if (event.key !== 'Tab') {
      return;
    }

    var focusables = authFocusables();
    if (focusables.length === 0) {
      event.preventDefault();
      return;
    }

    var first = focusables[0];
    var last = focusables[focusables.length - 1];
    var active = document.activeElement;

    if (event.shiftKey) {
      if (active === first || !el.authScreen.contains(active)) {
        event.preventDefault();
        last.focus();
      }
      return;
    }

    if (active === last || !el.authScreen.contains(active)) {
      event.preventDefault();
      first.focus();
    }
  }

  function authFocusables() {
    var nodes = el.authScreen.querySelectorAll(
      'button:not([disabled]), input:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])'
    );
    var focusables = [];
    for (var i = 0; i < nodes.length; i += 1) {
      if (nodes[i].getClientRects().length > 0) {
        focusables.push(nodes[i]);
      }
    }
    return focusables;
  }

  function focusAuthToken() {
    try {
      el.authToken.focus({ preventScroll: true });
    } catch (error) {
      el.authToken.focus();
    }
  }

  function focusMainHeading() {
    var target = el.pageTitle;
    if (state.view === 'events' && el.eventsTitle) {
      target = el.eventsTitle;
    } else if (state.view === 'failures' && el.failuresTitle) {
      target = el.failuresTitle;
    }
    if (!target) {
      return;
    }
    try {
      target.focus({ preventScroll: true });
    } catch (error) {
      target.focus();
    }
  }

  function lockApp(locked) {
    document.body.classList.toggle('is-auth-locked', locked);

    if (el.shell) {
      if ('inert' in el.shell) {
        el.shell.inert = locked;
      }
      if (locked) {
        el.shell.setAttribute('aria-hidden', 'true');
      } else {
        el.shell.removeAttribute('aria-hidden');
      }
    }

    if (el.skipLink && 'inert' in el.skipLink) {
      el.skipLink.inert = locked;
    }
  }

  function updateAuthChrome() {
    var showLogout = state.auth.authenticated && state.auth.authEnabled;
    el.logoutButton.hidden = !showLogout;
    el.mobileLogout.hidden = !showLogout;
    if (!showLogout) {
      setLogoutBusy(false);
    }
  }

  async function handleLogout(fromAlert) {
    if (state.auth.logoutBusy) {
      return;
    }

    var retrying = Boolean(fromAlert) && !el.logoutAlert.hidden;

    if (!retrying) {
      hideLogoutAlert();
    }

    setLogoutBusy(true);
    if (retrying) {
      setLogoutAlertBusy(true);
    }

    try {
      await requestJSON('/api/logout', {
        method: 'POST',
        headers: { 'Accept': 'application/json' },
        authCheck: true,
        requireStatus: 204
      });
    } catch (error) {
      // HttpOnly 会话只能由服务端清除；失败时保持登录态，避免“假退出”。
      if (!state.auth.authenticated) {
        return;
      }
      setLogoutBusy(false);
      showLogoutAlert(logoutFailureMessage(error));
      focusLogoutAlertRetry();
      return;
    }

    hideLogoutAlert();
    state.auth.authenticated = false;
    state.auth.generation += 1;
    stopDashboard();
    showLoginOverlay({ resetToken: true });
  }

  function logoutFailureMessage(error) {
    if (error && error.timeout) {
      return '退出登录超时，请重试。';
    }
    if (error && error.network) {
      return '连不上服务，没能退出登录。请重试。';
    }
    if (error && error.status === 403) {
      return '退出登录被拒绝，请重试。';
    }
    if (error && error.status >= 500) {
      return '服务暂时不可用，没能退出登录。请重试。';
    }
    return '没能退出登录，请重试。';
  }

  function showLogoutAlert(message) {
    el.logoutAlertText.textContent = message;
    el.logoutAlert.hidden = false;
    setLogoutAlertBusy(false);
  }

  function hideLogoutAlert() {
    el.logoutAlert.hidden = true;
    el.logoutAlertText.textContent = '';
    setLogoutAlertBusy(false);
  }

  function setLogoutAlertBusy(busy) {
    el.logoutAlertRetry.disabled = busy;
    el.logoutAlertDismiss.disabled = busy;
    el.logoutAlertRetry.textContent = busy ? '退出中…' : '重试';

    if (busy) {
      el.logoutAlertRetry.setAttribute('aria-busy', 'true');
    } else {
      el.logoutAlertRetry.removeAttribute('aria-busy');
    }
  }

  function focusLogoutAlertRetry() {
    try {
      el.logoutAlertRetry.focus({ preventScroll: true });
    } catch (error) {
      el.logoutAlertRetry.focus();
    }
  }

  function handleLogoutAlertDismiss() {
    hideLogoutAlert();
    focusLogoutControl();
  }

  function focusLogoutControl() {
    var target = narrowScreen.matches ? el.mobileLogout : el.logoutButton;

    if (!target || target.hidden) {
      target = el.logoutButton.hidden ? el.mobileLogout : el.logoutButton;
    }
    if (!target || target.hidden) {
      target = el.pageTitle;
    }
    if (!target) {
      return;
    }

    try {
      target.focus({ preventScroll: true });
    } catch (error) {
      target.focus();
    }
  }

  function setLogoutBusy(busy) {
    state.auth.logoutBusy = busy;
    el.logoutButton.disabled = busy;
    el.mobileLogout.disabled = busy;
    el.logoutLabel.textContent = busy ? '正在退出…' : '退出登录';

    if (busy) {
      el.logoutButton.setAttribute('aria-busy', 'true');
      el.mobileLogout.setAttribute('aria-busy', 'true');
    } else {
      el.logoutButton.removeAttribute('aria-busy');
      el.mobileLogout.removeAttribute('aria-busy');
    }
  }

  function handleUnauthorized() {
    if (!state.auth.authenticated) {
      return;
    }

    state.auth.authenticated = false;
    state.auth.authEnabled = true;
    state.auth.generation += 1;
    stopDashboard();
    showLoginOverlay({
      message: '登录已过期，请重新登录。',
      resetToken: true
    });
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

    // 登录层打开时只记录目标视图，不加载数据、不抢登录框焦点。
    if (!state.auth.authenticated) {
      return changed;
    }

    updateViewChrome(view);

    if (view !== 'events') {
      clearEventSelection(false);
    }
    if (view !== 'overview') {
      clearConsumerSelection(false);
      clearProducerSelection(false);
    }

    if (view === 'events') {
      state.failurePageFocus = '';
      cancelFailureAutoRefresh();
      ensureEventsView();
      applyEventColumnWidths();
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
      if (state.auth.authenticated && finePointer.matches) {
        el.captureText.focus({ preventScroll: true });
      }
    } catch (error) {
      if (error && error.detail) {
        console.warn('capture failed:', error.status || '', error.detail);
      }
      showCaptureError(captureFailureMessage(error));
      if (state.auth.authenticated && finePointer.matches) {
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
    if (state.streamSource !== null || !state.auth.authenticated) {
      return;
    }

    state.streamFailed = false;

    var source = new EventSource('/api/stream');
    state.streamSource = source;

    source.addEventListener('open', function () {
      if (state.streamSource === source) {
        handleStreamOpen();
      }
    });
    source.addEventListener('snapshot', function (message) {
      if (state.streamSource === source) {
        handleSnapshot(message);
      }
    });
    source.addEventListener('error', function () {
      if (state.streamSource === source) {
        handleStreamError();
      }
    });
  }

  function stopStream() {
    var source = state.streamSource;
    state.streamSource = null;

    if (source !== null) {
      source.close();
    }

    if (state.streamAuthProbeTimer !== null) {
      window.clearTimeout(state.streamAuthProbeTimer);
      state.streamAuthProbeTimer = null;
    }
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
    scheduleStreamAuthProbe();
  }

  function scheduleStreamAuthProbe() {
    if (!state.auth.authenticated ||
        !state.dashboardStarted ||
        state.streamAuthProbeTimer !== null ||
        state.streamAuthProbeBusy) {
      return;
    }

    var generation = state.auth.generation;

    state.streamAuthProbeTimer = window.setTimeout(async function () {
      state.streamAuthProbeTimer = null;

      if (!state.auth.authenticated ||
          !state.dashboardStarted ||
          state.auth.generation !== generation) {
        return;
      }

      state.streamAuthProbeBusy = true;
      try {
        var session = await fetchSession();
        if (state.auth.authenticated &&
            state.dashboardStarted &&
            state.auth.generation === generation &&
            session.authEnabled &&
            !session.authenticated) {
          handleUnauthorized();
        }
      } catch (error) {
        /* 探测失败不代表登录失效，保留 EventSource 的自动重连。 */
      } finally {
        state.streamAuthProbeBusy = false;
      }
    }, STREAM_AUTH_PROBE_MS);
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
      var decreased = previousTotal !== null && total < previousTotal;

      if (decreased && !state.pageLoading) {
        scheduleEventsRefresh(0, true);
      } else if (increased &&
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

    var shown = state.producers.slice(0, PRODUCER_LIMIT);
    pruneProducerSelection(shown);

    if (state.producers.length === 0) {
      el.producerList.textContent = '';
      el.producerList.hidden = true;
      el.producersMore.hidden = true;
      el.producersMore.textContent = '';
      el.producersEmpty.hidden = false;
      el.producersEmptyTitle.textContent = '还没有生产者';
      el.producersEmptyText.textContent = '发布事件后，这里会显示输入来源。';
      state.producerKey = '';
      updateProducerSelectionUi();
      return;
    }

    el.producersEmpty.hidden = true;
    el.producerList.hidden = false;

    var key = state.producers.map(function (producer) {
      return producer.source + '|' + producer.count + '|' + producer.lastAt;
    }).join('\u0000');

    if (key === state.producerKey) {
      updateProducerSelectionUi();
      return;
    }
    state.producerKey = key;

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

    updateProducerSelectionUi();
  }

  function createProducerRow(producer) {
    var row = document.createElement('li');
    row.className = 'producer-row';

    row.appendChild(createRowCheck(
      'producer-select',
      producer.source,
      state.selectedProducerSources.has(producer.source),
      '选择来源 ' + producer.source,
      '该来源缺少名称，无法选择'
    ));

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

    var actions = document.createElement('div');
    actions.className = 'producer-actions';
    actions.appendChild(createProducerHideButton(producer.source));
    actions.appendChild(createRowDeleteButton(
      'producer-delete',
      producer.source,
      '删除来源 ' + producer.source,
      '该来源缺少名称，无法删除',
      '删除来源 ' + producer.source + '，同时删除该来源的全部事件'
    ));
    row.appendChild(actions);

    return row;
  }

  function createProducerHideButton(source) {
    var button = document.createElement('button');
    button.type = 'button';
    button.className = 'row-delete row-hide producer-hide';

    if (source) {
      button.setAttribute('data-value', source);
      button.setAttribute('aria-label', '仅删除来源 ' + source + '，保留其事件');
      button.setAttribute('title', '仅删除来源 ' + source + '，保留其事件');
    } else {
      button.disabled = true;
      button.setAttribute('aria-label', '该来源缺少名称，无法删除');
      button.setAttribute('title', '该来源缺少名称，无法删除');
    }

    button.appendChild(createHideIcon());
    return button;
  }

  function createHideIcon() {
    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 20 20');
    svg.setAttribute('aria-hidden', 'true');
    svg.setAttribute('focusable', 'false');
    svg.setAttribute('fill', 'none');
    svg.setAttribute('stroke', 'currentColor');
    svg.setAttribute('stroke-width', '1.5');
    svg.setAttribute('stroke-linecap', 'round');
    svg.setAttribute('stroke-linejoin', 'round');

    [
      'M2.75 10s2.5-4.25 7.25-4.25S17.25 10 17.25 10s-2.5 4.25-7.25 4.25S2.75 10 2.75 10Z',
      'M8.15 8.15A2.5 2.5 0 0 0 10 12.5',
      'M3.5 3.5l13 13'
    ].forEach(function (pathData) {
      var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', pathData);
      svg.appendChild(path);
    });

    return svg;
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
    if (!state.dashboardStarted || !state.auth.authenticated) {
      return;
    }
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

  function readStoredColumnWidths() {
    var widths = {
      type: COLUMN_DEFAULTS.type,
      source: COLUMN_DEFAULTS.source,
      time: COLUMN_DEFAULTS.time
    };

    try {
      var raw = window.localStorage.getItem(COLUMN_WIDTH_KEY);
      if (raw) {
        var data = JSON.parse(raw);
        if (isRecord(data)) {
          ['type', 'source', 'time'].forEach(function (key) {
            var value = Number(data[key]);
            if (Number.isFinite(value)) {
              widths[key] = Math.min(
                Math.max(Math.round(value), COLUMN_MIN[key]),
                COLUMN_MAX[key]
              );
            }
          });
        }
      }
    } catch (error) {
      /* 隐私模式下 localStorage 可能不可用 */
    }

    return widths;
  }

  function storeColumnWidths() {
    try {
      window.localStorage.setItem(COLUMN_WIDTH_KEY, JSON.stringify(state.columnWidths));
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

  function scheduleEventsRefresh(delay, force) {
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
      if (!force && (state.page !== 1 || state.selectedType)) {
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
    clearEventSelection(false);

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
    clearEventSelection(false);

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
    clearEventSelection(false);

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
    clearEventSelection(false);

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
    clearEventSelection(false);

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
    clearEventSelection(false);
    el.eventsSelectAll.disabled = true;
    el.eventsSelectAllMobile.disabled = true;
    el.eventsSelection.hidden = true;
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
    clearEventSelection(false);
    el.eventsSelectAll.disabled = true;
    el.eventsSelectAllMobile.disabled = true;
    el.eventsSelection.hidden = true;
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
    pruneEventSelection();
    updateEventSelectionUi();
    applyEventColumnWidths();

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

    var deleteId = eventDeleteId(event);
    var rawSource = event && typeof event.source === 'string' ? event.source : '';
    var source = rawSource || '未知';
    var selectionKey = eventSelectionKey(event);

    if (deleteId) {
      row.setAttribute('data-event-id', deleteId);
    }
    if (rawSource) {
      row.setAttribute('data-event-source', rawSource);
    }

    var selectCell = document.createElement('td');
    selectCell.className = 'cell-select';
    selectCell.appendChild(createRowCheck(
      'event-row-select',
      selectionKey,
      selectionKey ? state.selectedEventIds.has(selectionKey) : false,
      '选择事件 ' + deleteId + '，来源 ' + source,
      '该事件缺少标识或来源，无法选择'
    ));
    row.appendChild(selectCell);

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
    time.textContent = completeTime(rawTime);
    time.setAttribute('title', full);
    time.setAttribute('aria-label', full);
    timeCell.appendChild(time);
    row.appendChild(timeCell);

    var actionsCell = document.createElement('td');
    actionsCell.className = 'cell-actions';
    actionsCell.appendChild(createRowDeleteButton(
      'event-row-delete',
      selectionKey,
      '删除事件 ' + deleteId + '，来源 ' + source,
      '该事件缺少标识或来源，无法删除',
      '删除事件'
    ));
    row.appendChild(actionsCell);

    // 行没有可用的 id 时仍可在详情里取到刚刚渲染的这份数据。
    state.detailRowData.set(row, event);

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
    pruneConsumerSelection();

    if (state.consumers.length === 0) {
      el.consumerList.textContent = '';
      el.consumerList.hidden = true;
      el.consumersEmpty.hidden = false;
      el.consumersEmptyTitle.textContent = '还没有消费者';
      el.consumersEmptyText.textContent = '注册消费者后，这里会显示游标和投递进度。';
      state.consumerKey = '';
      updateConsumerSelectionUi();
      return;
    }

    el.consumersEmpty.hidden = true;
    el.consumerList.hidden = false;

    var key = state.consumers.map(function (consumer) {
      return [
        consumer.name,
        Array.isArray(consumer.types) ? consumer.types.join(',') : '',
        Array.isArray(consumer.patterns) ? consumer.patterns.join(',') : '',
        consumer.cursor,
        consumer.pending,
        consumer.delivered,
        consumer.failed,
        consumer.last_at
      ].join('|');
    }).join('\u0000');

    if (key === state.consumerKey) {
      updateConsumerSelectionUi();
      return;
    }
    state.consumerKey = key;

    var fragment = document.createDocumentFragment();
    state.consumers.forEach(function (consumer) {
      fragment.appendChild(createConsumerRow(consumer));
    });

    el.consumerList.textContent = '';
    el.consumerList.appendChild(fragment);
    updateConsumerSelectionUi();
  }

  function createConsumerRow(consumer) {
    var row = document.createElement('div');
    row.className = 'consumer-row';

    var deleteName = consumer && typeof consumer.name === 'string' && consumer.name
      ? consumer.name
      : '';

    var head = document.createElement('div');
    head.className = 'consumer-head';
    head.appendChild(createRowCheck(
      'consumer-select',
      deleteName,
      deleteName ? state.selectedConsumerNames.has(deleteName) : false,
      '选择消费者 ' + deleteName,
      '该消费者缺少名称，无法选择'
    ));

    var name = document.createElement('p');
    name.className = 'consumer-name';
    var rawName = deleteName || '未命名消费者';
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
    head.appendChild(createRowDeleteButton(
      'consumer-delete',
      deleteName,
      '删除消费者 ' + deleteName,
      '该消费者缺少名称，无法删除',
      '删除消费者 ' + deleteName
    ));
    row.appendChild(head);

    var consumerTypes = consumer && Array.isArray(consumer.types) ? consumer.types : [];
    var consumerPatterns = consumer && Array.isArray(consumer.patterns) ? consumer.patterns : [];

    if (consumerTypes.length > 0 || consumerPatterns.length > 0) {
      var subscriptions = document.createElement('div');
      subscriptions.className = 'consumer-types';

      consumerTypes.forEach(function (type) {
        var raw = String(type);
        var tag = document.createElement('span');
        tag.className = 'type-tag';
        tag.textContent = raw;
        tag.setAttribute('title', raw);
        subscriptions.appendChild(tag);
      });

      consumerPatterns.forEach(function (pattern) {
        var raw = String(pattern);
        var tag = document.createElement('span');
        tag.className = 'type-tag is-regex';
        tag.textContent = 're:' + raw;
        tag.setAttribute('title', raw);
        subscriptions.appendChild(tag);
      });

      row.appendChild(subscriptions);
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

  /* ---- 选择、删除与列宽 ---- */

  function createDeleteIcon() {
    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 20 20');
    svg.setAttribute('aria-hidden', 'true');
    svg.setAttribute('focusable', 'false');
    svg.setAttribute('fill', 'none');
    svg.setAttribute('stroke', 'currentColor');
    svg.setAttribute('stroke-width', '1.5');
    svg.setAttribute('stroke-linecap', 'round');
    svg.setAttribute('stroke-linejoin', 'round');

    [
      'M4.5 5.75h11',
      'M8 5.75V4.5h4v1.25',
      'M6 5.75l.75 9h6.5L14 5.75',
      'M8.75 8.75v3.5',
      'M11.25 8.75v3.5'
    ].forEach(function (pathData) {
      var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', pathData);
      svg.appendChild(path);
    });

    return svg;
  }

  function createRowCheck(className, value, checked, label, disabledLabel) {
    var wrapper = document.createElement('label');
    wrapper.className = 'row-check';

    var input = document.createElement('input');
    input.type = 'checkbox';
    input.className = className;

    if (value) {
      input.checked = Boolean(checked);
      input.setAttribute('data-value', value);
      input.setAttribute('aria-label', label);
    } else {
      input.disabled = true;
      input.setAttribute('aria-label', disabledLabel);
    }

    wrapper.appendChild(input);
    return wrapper;
  }

  function createRowDeleteButton(className, value, label, disabledLabel, title) {
    var button = document.createElement('button');
    button.type = 'button';
    button.className = 'row-delete ' + className;

    if (value) {
      button.setAttribute('data-value', value);
      button.setAttribute('aria-label', label);
      button.setAttribute('title', title || label);
    } else {
      button.disabled = true;
      button.setAttribute('aria-label', disabledLabel);
      button.setAttribute('title', disabledLabel);
    }

    button.appendChild(createDeleteIcon());
    return button;
  }

  function eventDeleteId(event) {
    if (!event || event.id === undefined || event.id === null) {
      return '';
    }
    var id = String(event.id);
    return id;
  }

  function eventDeleteRef(event) {
    var id = eventDeleteId(event);
    var source = event && typeof event.source === 'string' ? event.source : '';
    if (!id || !source) {
      return null;
    }
    return { id: id, source: source };
  }

  function eventSelectionKey(event) {
    var id = eventDeleteId(event);
    var source = event && typeof event.source === 'string' ? event.source : '';
    if (!id || !source) {
      return '';
    }
    // Event IDs are scoped by source; keep both in the selection key.
    return JSON.stringify([source, id]);
  }

  function consumerDeleteName(consumer) {
    return consumer && typeof consumer.name === 'string' ? consumer.name : '';
  }

  function updateEventSelectionUi() {
    var validKeys = [];
    state.pageEvents.forEach(function (event) {
      var key = eventSelectionKey(event);
      if (key) {
        validKeys.push(key);
      }
    });

    var selectedCount = 0;
    validKeys.forEach(function (key) {
      if (state.selectedEventIds.has(key)) {
        selectedCount += 1;
      }
    });

    var boxes = el.eventRows.querySelectorAll('.event-row-select');
    for (var i = 0; i < boxes.length; i += 1) {
      var key = boxes[i].getAttribute('data-value') || '';
      boxes[i].checked = Boolean(key && state.selectedEventIds.has(key));
    }

    var allSelected = validKeys.length > 0 && selectedCount === validKeys.length;
    var partiallySelected = selectedCount > 0 && selectedCount < validKeys.length;

    [el.eventsSelectAll, el.eventsSelectAllMobile].forEach(function (input) {
      if (!input) {
        return;
      }
      input.checked = allSelected;
      input.indeterminate = partiallySelected;
      input.disabled = validKeys.length === 0;
    });

    el.eventsSelection.hidden = selectedCount === 0;
    el.eventsSelectionCount.textContent = '已选 ' + formatNumber(selectedCount) + ' 条';
  }

  function pruneEventSelection() {
    var validKeys = new Set();
    state.pageEvents.forEach(function (event) {
      var key = eventSelectionKey(event);
      if (key) {
        validKeys.add(key);
      }
    });

    state.selectedEventIds.forEach(function (key) {
      if (!validKeys.has(key)) {
        state.selectedEventIds.delete(key);
      }
    });
  }

  function clearEventSelection(restoreFocus) {
    var hadSelection = state.selectedEventIds.size > 0;
    state.selectedEventIds.clear();
    updateEventSelectionUi();
    if (hadSelection && restoreFocus) {
      focusSelectionFallback('events');
    }
  }

  function handleEventSelectAllChange(event) {
    var checked = Boolean(event.target.checked);
    var validKeys = [];
    state.pageEvents.forEach(function (item) {
      var key = eventSelectionKey(item);
      if (key) {
        validKeys.push(key);
      }
    });

    validKeys.forEach(function (key) {
      if (checked) {
        state.selectedEventIds.add(key);
      } else {
        state.selectedEventIds.delete(key);
      }
    });
    updateEventSelectionUi();
  }

  function handleEventRowSelectChange(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var input = event.target.closest('.event-row-select');
    if (!input || !el.eventRows.contains(input)) {
      return;
    }
    var key = input.getAttribute('data-value');
    if (!key) {
      return;
    }
    if (input.checked) {
      state.selectedEventIds.add(key);
    } else {
      state.selectedEventIds.delete(key);
    }
    updateEventSelectionUi();
  }

  function selectedPageEventRefs() {
    var refs = [];
    state.pageEvents.forEach(function (event) {
      var ref = eventDeleteRef(event);
      var key = eventSelectionKey(event);
      if (ref && key && state.selectedEventIds.has(key)) {
        refs.push(ref);
      }
    });
    return refs;
  }

  function openSelectedEventsDeleteDialog() {
    var refs = selectedPageEventRefs();
    if (refs.length === 0) {
      updateEventSelectionUi();
      return;
    }
    openDeleteConfirm('events', refs, el.eventsTitle);
  }

  function handleEventRowDeleteClick(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var button = event.target.closest('.event-row-delete');
    if (!button || !el.eventRows.contains(button)) {
      return;
    }
    var key = button.getAttribute('data-value');
    if (!key) {
      return;
    }

    var ref = null;
    state.pageEvents.forEach(function (item) {
      var candidate = eventDeleteRef(item);
      if (candidate && eventSelectionKey(candidate) === key) {
        ref = candidate;
      }
    });
    if (!ref) {
      pruneEventSelection();
      updateEventSelectionUi();
      return;
    }
    openDeleteConfirm('events', [ref], el.eventsTitle);
  }

  function updateConsumerSelectionUi() {
    var validNames = [];
    state.consumers.forEach(function (consumer) {
      var name = consumerDeleteName(consumer);
      if (name) {
        validNames.push(name);
      }
    });

    var selectedCount = 0;
    validNames.forEach(function (name) {
      if (state.selectedConsumerNames.has(name)) {
        selectedCount += 1;
      }
    });

    var boxes = el.consumerList.querySelectorAll('.consumer-select');
    for (var i = 0; i < boxes.length; i += 1) {
      var name = boxes[i].getAttribute('data-value') || '';
      boxes[i].checked = Boolean(name && state.selectedConsumerNames.has(name));
    }

    el.consumersSelectAll.checked = validNames.length > 0 && selectedCount === validNames.length;
    el.consumersSelectAll.indeterminate = selectedCount > 0 && selectedCount < validNames.length;
    el.consumersSelectAll.disabled = validNames.length === 0;
    el.consumersSelection.hidden = selectedCount === 0;
    el.consumersSelectionCount.textContent = '已选 ' + formatNumber(selectedCount) + ' 个消费者';
  }

  function pruneConsumerSelection() {
    var validNames = new Set();
    state.consumers.forEach(function (consumer) {
      var name = consumerDeleteName(consumer);
      if (name) {
        validNames.add(name);
      }
    });
    state.selectedConsumerNames.forEach(function (name) {
      if (!validNames.has(name)) {
        state.selectedConsumerNames.delete(name);
      }
    });
  }

  function clearConsumerSelection(restoreFocus) {
    var hadSelection = state.selectedConsumerNames.size > 0;
    state.selectedConsumerNames.clear();
    updateConsumerSelectionUi();
    if (hadSelection && restoreFocus) {
      focusSelectionFallback('consumers');
    }
  }

  function handleConsumerSelectAllChange(event) {
    var checked = Boolean(event.target.checked);
    state.consumers.forEach(function (consumer) {
      var name = consumerDeleteName(consumer);
      if (!name) {
        return;
      }
      if (checked) {
        state.selectedConsumerNames.add(name);
      } else {
        state.selectedConsumerNames.delete(name);
      }
    });
    updateConsumerSelectionUi();
  }

  function handleConsumerSelectChange(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var input = event.target.closest('.consumer-select');
    if (!input || !el.consumerList.contains(input)) {
      return;
    }
    var name = input.getAttribute('data-value');
    if (!name) {
      return;
    }
    if (input.checked) {
      state.selectedConsumerNames.add(name);
    } else {
      state.selectedConsumerNames.delete(name);
    }
    updateConsumerSelectionUi();
  }

  function openSelectedConsumersDeleteDialog() {
    var names = [];
    state.consumers.forEach(function (consumer) {
      var name = consumerDeleteName(consumer);
      if (name && state.selectedConsumerNames.has(name)) {
        names.push(name);
      }
    });
    if (names.length === 0) {
      updateConsumerSelectionUi();
      return;
    }
    openDeleteConfirm('consumers', names, el.consumersTitle);
  }

  function handleConsumerDeleteClick(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var button = event.target.closest('.consumer-delete');
    if (!button || !el.consumerList.contains(button)) {
      return;
    }
    var name = button.getAttribute('data-value');
    if (!name) {
      return;
    }
    openDeleteConfirm('consumers', [name], el.consumersTitle);
  }

  function visibleProducers() {
    return state.producers.slice(0, PRODUCER_LIMIT);
  }

  function updateProducerSelectionUi() {
    var validSources = [];
    visibleProducers().forEach(function (producer) {
      if (producer.source) {
        validSources.push(producer.source);
      }
    });

    var selectedCount = 0;
    validSources.forEach(function (source) {
      if (state.selectedProducerSources.has(source)) {
        selectedCount += 1;
      }
    });

    var boxes = el.producerList.querySelectorAll('.producer-select');
    for (var i = 0; i < boxes.length; i += 1) {
      var source = boxes[i].getAttribute('data-value') || '';
      boxes[i].checked = Boolean(source && state.selectedProducerSources.has(source));
    }

    el.producersSelectAll.checked = validSources.length > 0 && selectedCount === validSources.length;
    el.producersSelectAll.indeterminate = selectedCount > 0 && selectedCount < validSources.length;
    el.producersSelectAll.disabled = validSources.length === 0;
    el.producersSelection.hidden = selectedCount === 0;
    el.producersSelectionCount.textContent = '已选 ' + formatNumber(selectedCount) + ' 个来源';
  }

  function pruneProducerSelection(shown) {
    var validSources = new Set();
    (shown || []).forEach(function (producer) {
      if (producer.source) {
        validSources.add(producer.source);
      }
    });
    state.selectedProducerSources.forEach(function (source) {
      if (!validSources.has(source)) {
        state.selectedProducerSources.delete(source);
      }
    });
  }

  function clearProducerSelection(restoreFocus) {
    var hadSelection = state.selectedProducerSources.size > 0;
    state.selectedProducerSources.clear();
    updateProducerSelectionUi();
    if (hadSelection && restoreFocus) {
      focusSelectionFallback('producers');
    }
  }

  function handleProducerSelectAllChange(event) {
    var checked = Boolean(event.target.checked);
    visibleProducers().forEach(function (producer) {
      if (!producer.source) {
        return;
      }
      if (checked) {
        state.selectedProducerSources.add(producer.source);
      } else {
        state.selectedProducerSources.delete(producer.source);
      }
    });
    updateProducerSelectionUi();
  }

  function handleProducerSelectChange(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var input = event.target.closest('.producer-select');
    if (!input || !el.producerList.contains(input)) {
      return;
    }
    var source = input.getAttribute('data-value');
    if (!source) {
      return;
    }
    if (input.checked) {
      state.selectedProducerSources.add(source);
    } else {
      state.selectedProducerSources.delete(source);
    }
    updateProducerSelectionUi();
  }

  function openSelectedProducersDeleteDialog() {
    var sources = [];
    visibleProducers().forEach(function (producer) {
      if (producer.source && state.selectedProducerSources.has(producer.source)) {
        sources.push(producer.source);
      }
    });
    if (sources.length === 0) {
      updateProducerSelectionUi();
      return;
    }
    openDeleteConfirm('producers', sources, el.producersTitle);
  }

  function handleProducerDeleteClick(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var button = event.target.closest('.producer-delete');
    if (!button || !el.producerList.contains(button)) {
      return;
    }
    var source = button.getAttribute('data-value');
    if (!source) {
      return;
    }
    openDeleteConfirm('producers', [source], el.producersTitle);
  }

  function handleProducerHideClick(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var button = event.target.closest('.producer-hide');
    if (!button || !el.producerList.contains(button)) {
      return;
    }
    var source = button.getAttribute('data-value');
    if (!source) {
      return;
    }
    openDeleteConfirm('producers-hide', [source], el.producersTitle);
  }

  function clearAllSelections() {
    clearEventSelection(false);
    clearConsumerSelection(false);
    clearProducerSelection(false);
  }

  function focusSelectionFallback(kind) {
    var target = null;

    if (kind === 'events') {
      if (isElementFocusable(el.eventsSelectAll)) {
        target = el.eventsSelectAll;
      } else if (isElementFocusable(el.eventsSelectAllMobile)) {
        target = el.eventsSelectAllMobile;
      } else {
        target = el.eventsTitle;
      }
    } else if (kind === 'consumers') {
      target = isElementFocusable(el.consumersSelectAll) ? el.consumersSelectAll : el.consumersTitle;
    } else {
      target = isElementFocusable(el.producersSelectAll) ? el.producersSelectAll : el.producersTitle;
    }

    focusElement(target);
  }

  function isElementFocusable(target) {
    return Boolean(target) &&
      target.isConnected &&
      !target.disabled &&
      target.getClientRects().length > 0;
  }

  function focusElement(target) {
    if (!target) {
      return;
    }
    try {
      target.focus({ preventScroll: true });
    } catch (error) {
      target.focus();
    }
  }

  /* ---- 列宽 ---- */

  function applyEventColumnWidths() {
    if (!el.eventTable) {
      return;
    }

    ['type', 'source', 'time'].forEach(function (key) {
      var width = state.columnWidths[key];
      var col = el.eventCols && el.eventCols[key];
      if (col) {
        col.style.width = width + 'px';
      }

      var resizer = el.eventResizers && el.eventResizers[key];
      if (resizer) {
        resizer.setAttribute('aria-valuenow', String(width));
        resizer.setAttribute('aria-valuemin', String(COLUMN_MIN[key]));
        resizer.setAttribute('aria-valuemax', String(COLUMN_MAX[key]));
        resizer.setAttribute('aria-valuetext', width + ' 像素');
      }
    });

    var minWidth = SELECT_COL_WIDTH + ACTIONS_COL_WIDTH +
      state.columnWidths.type + state.columnWidths.source + state.columnWidths.time +
      CONTENT_MIN_WIDTH;
    el.eventTable.style.minWidth = narrowScreen.matches ? '' : minWidth + 'px';

    var contentResizer = el.eventResizers && el.eventResizers.content;
    if (contentResizer) {
      var contentHead = el.eventTable.querySelector('th[data-column="content"]');
      var contentWidth = contentHead
        ? Math.round(contentHead.getBoundingClientRect().width)
        : 0;
      var availableContent = el.tableWrap && el.tableWrap.clientWidth
        ? el.tableWrap.clientWidth - SELECT_COL_WIDTH - ACTIONS_COL_WIDTH -
          state.columnWidths.type - state.columnWidths.source - COLUMN_MIN.time
        : contentWidth;
      var maxContent = Math.max(CONTENT_MIN_WIDTH, availableContent);

      contentResizer.setAttribute('aria-valuenow', String(contentWidth));
      contentResizer.setAttribute('aria-valuemin', String(CONTENT_MIN_WIDTH));
      contentResizer.setAttribute('aria-valuemax', String(Math.round(maxContent)));
      contentResizer.setAttribute('aria-valuetext', contentWidth + ' 像素');
    }
  }

  function handleColumnResizeStart(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var resizer = event.target.closest('.col-resizer');
    if (!resizer || !el.eventTable.contains(resizer)) {
      return;
    }
    if (event.pointerType === 'mouse' && event.button !== 0) {
      return;
    }

    var column = resizer.getAttribute('data-column');
    var isContent = column === 'content';
    if (!isContent && !Object.prototype.hasOwnProperty.call(COLUMN_DEFAULTS, column)) {
      return;
    }

    event.preventDefault();
    state.columnDrag = {
      column: column,
      isContent: isContent,
      startX: event.clientX,
      startWidth: isContent ? state.columnWidths.time : state.columnWidths[column],
      pointerId: event.pointerId,
      resizer: resizer
    };
    document.body.classList.add('is-col-resizing');

    try {
      resizer.setPointerCapture(event.pointerId);
    } catch (error) {
      /* 某些浏览器不支持 pointer capture，document 监听仍可工作 */
    }
  }

  function handleColumnResizeMove(event) {
    var drag = state.columnDrag;
    if (!drag || event.pointerId !== drag.pointerId) {
      return;
    }

    event.preventDefault();
    var delta = event.clientX - drag.startX;

    if (drag.isContent) {
      var timeWidth = drag.startWidth - delta;
      state.columnWidths.time = Math.round(Math.min(
        Math.max(timeWidth, COLUMN_MIN.time),
        COLUMN_MAX.time
      ));
    } else {
      var width = drag.startWidth + delta;
      width = Math.min(Math.max(width, COLUMN_MIN[drag.column]), COLUMN_MAX[drag.column]);
      state.columnWidths[drag.column] = Math.round(width);
    }

    applyEventColumnWidths();
  }

  function handleColumnResizeEnd(event) {
    var drag = state.columnDrag;
    if (!drag) {
      return;
    }
    if (event && event.pointerId !== drag.pointerId) {
      return;
    }

    state.columnDrag = null;
    document.body.classList.remove('is-col-resizing');

    try {
      if (drag.resizer.hasPointerCapture(drag.pointerId)) {
        drag.resizer.releasePointerCapture(drag.pointerId);
      }
    } catch (error) {
      /* 忽略释放失败的 pointer capture */
    }

    storeColumnWidths();
  }

  function handleColumnResizeKeydown(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    var resizer = event.target.closest('.col-resizer');
    if (!resizer || !el.eventTable.contains(resizer)) {
      return;
    }

    var column = resizer.getAttribute('data-column');
    var isContent = column === 'content';
    if (!isContent && !Object.prototype.hasOwnProperty.call(COLUMN_DEFAULTS, column)) {
      return;
    }

    var step = event.shiftKey ? 48 : 16;

    if (isContent) {
      var timeWidth = state.columnWidths.time;

      if (event.key === 'ArrowLeft') {
        timeWidth += step;
      } else if (event.key === 'ArrowRight') {
        timeWidth -= step;
      } else if (event.key === 'Home') {
        timeWidth = COLUMN_MAX.time;
      } else if (event.key === 'End') {
        timeWidth = COLUMN_MIN.time;
      } else {
        return;
      }

      event.preventDefault();
      state.columnWidths.time = Math.min(
        Math.max(timeWidth, COLUMN_MIN.time),
        COLUMN_MAX.time
      );
      applyEventColumnWidths();
      storeColumnWidths();
      return;
    }

    var width = state.columnWidths[column];

    if (event.key === 'ArrowLeft') {
      width -= step;
    } else if (event.key === 'ArrowRight') {
      width += step;
    } else if (event.key === 'Home') {
      width = COLUMN_MIN[column];
    } else if (event.key === 'End') {
      width = COLUMN_MAX[column];
    } else {
      return;
    }

    event.preventDefault();
    state.columnWidths[column] = Math.min(
      Math.max(width, COLUMN_MIN[column]),
      COLUMN_MAX[column]
    );
    applyEventColumnWidths();
    storeColumnWidths();
  }

  /* ---- 删除确认 ---- */

  function openDeleteConfirm(kind, items, fallback) {
    if (state.confirm || el.confirmDialog.open || el.eventDetailDialog.open) {
      return;
    }

    var content = confirmContentFor(kind, items);
    state.confirm = {
      kind: kind,
      items: items.slice(),
      returnFocus: document.activeElement,
      fallback: fallback || el.pageTitle,
      submitLabel: content.submitLabel || '删除',
      busyLabel: content.busyLabel || '删除中…',
      danger: content.danger !== false
    };
    state.deleteBusy = false;

    el.confirmTitle.textContent = content.title;
    el.confirmMessage.textContent = content.message;
    el.confirmWarning.textContent = content.warning;
    el.confirmWarning.hidden = !content.warning;
    el.confirmSubmit.classList.toggle('button-danger', state.confirm.danger);
    el.confirmSubmit.classList.toggle('button-primary', !state.confirm.danger);
    clearConfirmError();
    setConfirmBusy(false);
    el.deleteStatus.textContent = '';

    el.confirmDialog.showModal();
    focusElement(el.confirmCancel);
  }

  function confirmContentFor(kind, items) {
    var count = items.length;

    if (kind === 'events') {
      return {
        title: count === 1 ? '删除事件' : '删除所选事件',
        message: count === 1
          ? '确定要删除这条事件吗？'
          : '确定要删除选中的 ' + formatNumber(count) + ' 条事件吗？',
        warning: '删除后无法恢复。'
      };
    }

    if (kind === 'consumers') {
      return {
        title: count === 1 ? '删除消费者' : '删除所选消费者',
        message: count === 1
          ? '确定要删除消费者「' + items[0] + '」吗？'
          : '确定要删除选中的 ' + formatNumber(count) + ' 个消费者吗？',
        warning: '消费者会被取消注册，已有事件不会删除。'
      };
    }

    if (kind === 'producers-hide') {
      return {
        title: count === 1 ? '仅删除生产者' : '仅删除所选生产者',
        message: count === 1
          ? '确定要把来源「' + items[0] + '」从生产者列表删除吗？'
          : '确定要把选中的 ' + formatNumber(count) + ' 个来源从生产者列表删除吗？',
        warning: '只删除生产者条目，不删除它的事件。该来源再次发布事件时会重新出现。',
        submitLabel: '仅删来源',
        busyLabel: '正在移除…',
        danger: false
      };
    }

    return {
      title: count === 1 ? '删除来源' : '删除所选来源',
      message: count === 1
        ? '确定要删除来源「' + items[0] + '」吗？'
        : '确定要删除选中的 ' + formatNumber(count) + ' 个来源吗？',
      warning: count === 1
        ? '删除该来源会一并删除它的全部事件，且无法恢复。'
        : '删除这些来源会一并删除它们的全部事件，且无法恢复。'
    };
  }

  async function performConfirmedDelete() {
    var confirm = state.confirm;
    if (!confirm || state.deleteBusy) {
      return;
    }

    if (!confirm.items || confirm.items.length === 0) {
      showConfirmError('没有可删除的项目。');
      return;
    }
    if (confirm.items.length > MAX_DELETE_BATCH) {
      showConfirmError('一次最多删除 1000 项，请分批操作。');
      return;
    }

    var request = deleteRequestFor(confirm.kind, confirm.items);
    if (!request) {
      showConfirmError('没有可删除的项目。');
      return;
    }

    clearConfirmError();
    setConfirmBusy(true);

    try {
      var payload = await requestJSON(request.url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(request.body)
      });

      if (!state.auth.authenticated) {
        setConfirmBusy(false);
        return;
      }
      if (!isRecord(payload)) {
        throw invalidPayload('delete');
      }
      var deleted = Number(confirm.kind === 'producers-hide'
        ? payload.hidden : payload.deleted);
      if (!Number.isFinite(deleted) || deleted < 0) {
        throw invalidPayload('delete');
      }

      finishConfirmedDelete(confirm, deleted);
    } catch (error) {
      if (!state.auth.authenticated) {
        setConfirmBusy(false);
        return;
      }
      setConfirmBusy(false);
      showConfirmError(deleteFailureMessage(error));
      focusElement(el.confirmSubmit);
    }
  }

  function deleteRequestFor(kind, items) {
    if (kind === 'events') {
      var events = [];
      items.forEach(function (item) {
        if (item && item.id && item.source) {
          events.push({ id: item.id, source: item.source });
        }
      });
      return events.length > 0
        ? { url: '/api/events/delete', body: { events: events } }
        : null;
    }
    if (kind === 'consumers') {
      var names = items.filter(function (name) {
        return typeof name === 'string' && name !== '';
      });
      return names.length > 0
        ? { url: '/api/consumers/delete', body: { names: names } }
        : null;
    }
    if (kind === 'producers') {
      var sources = items.filter(function (source) {
        return typeof source === 'string' && source !== '';
      });
      return sources.length > 0
        ? { url: '/api/producers/delete', body: { sources: sources } }
        : null;
    }
    if (kind === 'producers-hide') {
      var hidden = items.filter(function (source) {
        return typeof source === 'string' && source !== '';
      });
      return hidden.length > 0
        ? { url: '/api/producers/hide', body: { sources: hidden } }
        : null;
    }
    return null;
  }

  function finishConfirmedDelete(confirm, deleted) {
    var kind = confirm.kind;

    if (kind === 'events') {
      clearEventSelection(false);
      cancelPageLoad();
      loadEventsPage({ silent: true });
    } else if (kind === 'consumers') {
      var removedNames = new Set(confirm.items);
      clearConsumerSelection(false);
      state.consumers = state.consumers.filter(function (consumer) {
        return !removedNames.has(consumerDeleteName(consumer));
      });
      applyKpis();
      renderConsumers();
    } else if (kind === 'producers' || kind === 'producers-hide') {
      var removedSources = new Set(confirm.items);
      clearProducerSelection(false);
      state.producers = state.producers.filter(function (producer) {
        return !removedSources.has(producer.source);
      });
      renderProducers();
    }

    setDeleteStatus(deleteSuccessMessage(kind, deleted, confirm.items.length));
    closeConfirmDialog({ focus: confirm.fallback });
  }

  function deleteFailureMessage(error) {
    if (error && error.timeout) {
      return '删除请求超时，请重试。';
    }
    if (error && error.network) {
      return '连不上服务，请检查连接后重试。';
    }
    if (error && error.status === 400) {
      return '删除请求没有通过检查，请刷新后重试。';
    }
    if (error && error.status === 403) {
      return '没有权限执行删除，请刷新后重试。';
    }
    if (error && error.status === 404) {
      return '部分内容已不存在，请刷新后重试。';
    }
    if (error && error.status >= 500) {
      return '服务暂时不可用，请稍后重试。';
    }
    return '删除失败，请重试。';
  }

  function deleteSuccessMessage(kind, deleted, requestedCount) {
    if (deleted <= 0) {
      return '没有可删除的项目，列表已刷新。';
    }
    if (kind === 'events') {
      return '已删除 ' + formatNumber(deleted) + ' 条事件。';
    }
    if (kind === 'consumers') {
      return '已删除 ' + formatNumber(deleted) + ' 个消费者。';
    }
    if (kind === 'producers-hide') {
      return '已从生产者列表删除 ' + formatNumber(requestedCount) +
        ' 个来源，事件保留。';
    }
    // 生产者删除接口返回的是删除的事件数，来源数量以请求为准。
    return '已删除 ' + formatNumber(requestedCount) + ' 个来源，共 ' +
      formatNumber(deleted) + ' 条事件。';
  }

  function setDeleteStatus(message) {
    el.deleteStatus.textContent = message;
  }

  function showConfirmError(message) {
    el.confirmErrorText.textContent = message;
    el.confirmError.hidden = false;
  }

  function clearConfirmError() {
    el.confirmError.hidden = true;
    el.confirmErrorText.textContent = '';
  }

  function setConfirmBusy(busy) {
    state.deleteBusy = busy;
    var confirm = state.confirm;
    var label = confirm && confirm.submitLabel ? confirm.submitLabel : '删除';
    var busyLabel = confirm && confirm.busyLabel ? confirm.busyLabel : '删除中…';
    el.confirmSubmit.disabled = busy;
    el.confirmCancel.disabled = busy;
    el.confirmSubmit.setAttribute('aria-busy', busy ? 'true' : 'false');
    el.confirmSubmit.textContent = busy ? busyLabel : label;
  }

  function closeConfirmDialog(options) {
    var opts = options || {};
    var confirm = state.confirm;

    if (!confirm && !el.confirmDialog.open) {
      return;
    }

    if (confirm) {
      if (opts.restoreFocus === false) {
        confirm.closeFocus = null;
      } else if (opts.focus) {
        confirm.closeFocus = opts.focus;
      } else {
        confirm.closeFocus = isElementFocusable(confirm.returnFocus)
          ? confirm.returnFocus
          : confirm.fallback;
      }
    }

    if (el.confirmDialog.open) {
      el.confirmDialog.close();
    } else {
      state.confirm = null;
      setConfirmBusy(false);
    }
  }

  function handleConfirmCancel(event) {
    if (state.deleteBusy) {
      event.preventDefault();
    }
  }

  function handleConfirmClose() {
    var confirm = state.confirm;
    state.confirm = null;
    setConfirmBusy(false);
    clearConfirmError();

    if (!confirm) {
      return;
    }

    var target = confirm.closeFocus;
    if (target === undefined) {
      target = isElementFocusable(confirm.returnFocus)
        ? confirm.returnFocus
        : confirm.fallback;
    }
    focusElement(target);
  }

  /* ---- 事件详情 ---- */

  function handleEventRowDoubleClick(event) {
    if (!(event.target instanceof Element)) {
      return;
    }
    if (event.target.closest('input, label, button, a, select, textarea, [contenteditable="true"]')) {
      return;
    }
    var row = event.target.closest('tr');
    if (!row || row.parentNode !== el.eventRows) {
      return;
    }
    var item = eventForRow(row);
    if (!item) {
      return;
    }
    openEventDetail(item, row);
  }

  function eventForRow(row) {
    var item = state.detailRowData.get(row);
    if (item) {
      return item;
    }
    var rows = el.eventRows.children;
    for (var i = 0; i < rows.length; i += 1) {
      if (rows[i] === row) {
        return state.pageEvents[i] || null;
      }
    }
    return null;
  }

  function openEventDetail(item, row) {
    if (!item || state.confirm || el.confirmDialog.open || el.eventDetailDialog.open) {
      return;
    }

    state.detail = {
      row: row,
      returnFocus: document.activeElement
    };

    renderEventDetail(item);

    if (row && !row.hasAttribute('tabindex')) {
      row.setAttribute('tabindex', '-1');
    }

    el.eventDetailDialog.showModal();
    focusElement(el.eventDetailClose);
  }

  function renderEventDetail(item) {
    resetEventDetailStatus();

    var types = eventTypes(item);
    el.eventDetailTypes.textContent = '';
    if (types.length === 0) {
      var emptyChip = document.createElement('span');
      emptyChip.className = 'type-chip';
      emptyChip.textContent = '—';
      el.eventDetailTypes.appendChild(emptyChip);
    } else {
      types.forEach(function (type) {
        var chip = document.createElement('span');
        chip.className = 'type-chip';
        chip.textContent = type;
        chip.setAttribute('title', type);
        el.eventDetailTypes.appendChild(chip);
      });
    }

    var rawSource = item && typeof item.source === 'string' ? item.source : '';
    el.eventDetailSource.textContent = rawSource || '未知';

    var id = eventDeleteId(item);
    el.eventDetailId.textContent = id || '—';

    var rawTime = item && typeof item.time === 'string' ? item.time : '';
    var parsed = rawTime ? Date.parse(rawTime) : NaN;
    if (rawTime && !Number.isNaN(parsed)) {
      el.eventDetailTime.textContent = fullTime(rawTime);
      el.eventDetailTime.setAttribute('datetime', rawTime);
    } else {
      el.eventDetailTime.textContent = rawTime ? '时间未知' : '—';
      el.eventDetailTime.removeAttribute('datetime');
    }

    if (rawTime) {
      el.eventDetailTimeRaw.textContent = '原始 · ' + rawTime;
      el.eventDetailTimeRaw.hidden = false;
    } else {
      el.eventDetailTimeRaw.textContent = '';
      el.eventDetailTimeRaw.hidden = true;
    }

    el.eventDetailJson.textContent = prettyJSON(item && item.data);
    el.eventDetailBody.scrollTop = 0;
    el.eventDetailJson.scrollTop = 0;
  }

  function closeEventDetailDialog() {
    if (el.eventDetailDialog.open) {
      el.eventDetailDialog.close();
    }
  }

  function handleEventDetailPointerDown(event) {
    state.detailBackdropDown = event.target === el.eventDetailDialog;
  }

  function handleEventDetailClick(event) {
    if (event.target !== el.eventDetailDialog) {
      state.detailBackdropDown = false;
      return;
    }
    if (!state.detailBackdropDown) {
      return;
    }
    state.detailBackdropDown = false;
    closeEventDetailDialog();
  }

  function handleEventDetailClose() {
    var detail = state.detail;
    state.detail = null;
    state.detailBackdropDown = false;
    resetEventDetailStatus();

    if (!detail) {
      return;
    }

    var target = null;
    if (isElementFocusable(detail.row)) {
      target = detail.row;
    } else if (detail.returnFocus &&
        detail.returnFocus !== document.body &&
        detail.returnFocus !== document.documentElement &&
        isElementFocusable(detail.returnFocus)) {
      target = detail.returnFocus;
    } else {
      target = el.eventsTitle;
    }
    focusElement(target);
  }

  function copyEventDetailJson() {
    var text = el.eventDetailJson.textContent || '';
    if (!text) {
      showEventDetailStatus('没有可复制的内容。', true);
      return;
    }
    copyTextToClipboard(text).then(function (ok) {
      if (!state.detail) {
        return;
      }
      showEventDetailStatus(ok ? '已复制完整 JSON。' : '复制失败，请手动选择内容。', !ok);
    });
  }

  function showEventDetailStatus(message, negative) {
    el.eventDetailStatus.textContent = message;
    el.eventDetailStatus.classList.toggle('is-negative', Boolean(negative));
    if (state.detailStatusTimer) {
      window.clearTimeout(state.detailStatusTimer);
    }
    state.detailStatusTimer = window.setTimeout(function () {
      state.detailStatusTimer = null;
      el.eventDetailStatus.textContent = '';
      el.eventDetailStatus.classList.remove('is-negative');
    }, 2600);
  }

  function resetEventDetailStatus() {
    if (state.detailStatusTimer) {
      window.clearTimeout(state.detailStatusTimer);
      state.detailStatusTimer = null;
    }
    el.eventDetailStatus.textContent = '';
    el.eventDetailStatus.classList.remove('is-negative');
  }

  function copyTextToClipboard(text) {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      return navigator.clipboard.writeText(text).then(function () {
        return true;
      }, function () {
        return fallbackCopyText(text);
      });
    }
    return Promise.resolve(fallbackCopyText(text));
  }

  function fallbackCopyText(text) {
    var area = document.createElement('textarea');
    area.value = text;
    area.setAttribute('readonly', '');
    area.style.position = 'fixed';
    area.style.top = '0';
    area.style.left = '-9999px';
    document.body.appendChild(area);

    var copied = false;
    try {
      area.select();
      copied = document.execCommand('copy');
    } catch (error) {
      copied = false;
    }
    document.body.removeChild(area);
    return copied;
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
    clearConsumerSelection(false);
    clearProducerSelection(false);
    el.consumersSelectAll.disabled = true;
    el.producersSelectAll.disabled = true;
    el.consumersSelection.hidden = true;
    el.producersSelection.hidden = true;

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
    clearConsumerSelection(false);
    clearProducerSelection(false);
    el.consumersSelectAll.disabled = true;
    el.producersSelectAll.disabled = true;
    el.consumersSelection.hidden = true;
    el.producersSelection.hidden = true;

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
    var authCheck = Boolean(options && options.authCheck);
    var requireStatus = options && typeof options.requireStatus === 'number'
      ? options.requireStatus
      : null;
    var authGeneration = state.auth.generation;
    var controller = new AbortController();
    var timeoutId = window.setTimeout(function () {
      controller.abort();
    }, REQUEST_TIMEOUT_MS);

    var request = Object.assign({}, options || {});
    delete request.authCheck;
    delete request.requireStatus;
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
        if (response.status === 401 &&
            !authCheck &&
            state.auth.generation === authGeneration) {
          handleUnauthorized();
        }
        throw apiError;
      }

      if (requireStatus !== null && response.status !== requireStatus) {
        var statusError = new Error('unexpected status');
        statusError.status = response.status;
        throw statusError;
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

  function prettyJSON(value) {
    if (value === undefined) {
      return '{}';
    }
    try {
      var text = JSON.stringify(value, null, 2);
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

  function completeTime(raw) {
    var time = Date.parse(raw);
    if (Number.isNaN(time)) {
      return '—';
    }
    var date = new Date(time);
    return date.getFullYear() + '-' +
      pad2(date.getMonth() + 1) + '-' +
      pad2(date.getDate()) + ' ' +
      pad2(date.getHours()) + ':' +
      pad2(date.getMinutes()) + ':' +
      pad2(date.getSeconds());
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
