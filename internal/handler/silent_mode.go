package handler

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"miaomiaowu/internal/auth"
	"miaomiaowu/internal/logger"
	"miaomiaowu/internal/storage"
)

var globalSilentModeManager *SilentModeManager

type SilentModeManager struct {
	repo                 *storage.TrafficRepository
	tokens               *auth.TokenStore
	lastActiveTime       sync.Map   // username -> time.Time
	lastGlobalActiveTime time.Time  // 全局活跃时间，任何用户获取订阅后更新
	globalActiveMu       sync.Mutex // 保护 lastGlobalActiveTime
	startTime            time.Time  // 服务启动时间，用于启动后临时恢复
	shortLinkSet         map[string]struct{}
	shortLinkSetMu       sync.RWMutex
	shortLinkSetTime     time.Time
}

func NewSilentModeManager(repo *storage.TrafficRepository, tokens *auth.TokenStore) *SilentModeManager {
	m := &SilentModeManager{
		repo:      repo,
		tokens:    tokens,
		startTime: time.Now(),
	}
	globalSilentModeManager = m
	logger.Info("🔓 [SILENT_MODE] 服务启动，静默模式临时恢复中",
		"start_time", m.startTime.Format("2006-01-02 15:04:05"),
	)
	return m
}

func GetSilentModeManager() *SilentModeManager {
	return globalSilentModeManager
}

// InvalidateShortLinkCache 使短链接缓存失效，下次请求时重新加载
func (m *SilentModeManager) InvalidateShortLinkCache() {
	m.shortLinkSetMu.Lock()
	m.shortLinkSetTime = time.Time{}
	m.shortLinkSetMu.Unlock()
}

func (m *SilentModeManager) refreshShortLinkSet() {
	ctx := context.Background()
	fileCodes, err := m.repo.GetAllFileShortCodes(ctx)
	if err != nil {
		return
	}
	userCodes, err := m.repo.GetAllUserShortCodes(ctx)
	if err != nil {
		return
	}

	set := make(map[string]struct{}, len(fileCodes)*len(userCodes))
	for fc := range fileCodes {
		for uc := range userCodes {
			set[fc+uc] = struct{}{}
		}
	}

	m.shortLinkSetMu.Lock()
	m.shortLinkSet = set
	m.shortLinkSetTime = time.Now()
	m.shortLinkSetMu.Unlock()
}

func (m *SilentModeManager) isKnownShortLink(path string) bool {
	if len(path) < 2 || !isAlphanumericPath(path) {
		return false
	}

	m.shortLinkSetMu.RLock()
	expired := time.Since(m.shortLinkSetTime) > 60*time.Second
	m.shortLinkSetMu.RUnlock()

	if expired {
		m.refreshShortLinkSet()
	}

	m.shortLinkSetMu.RLock()
	_, ok := m.shortLinkSet[path]
	m.shortLinkSetMu.RUnlock()
	return ok
}

func (m *SilentModeManager) RecordSubscriptionAccess(username string) {
	if username == "" {
		return
	}
	m.lastActiveTime.Store(username, time.Now())
	logger.Info("🔓 [SILENT_MODE] 用户获取订阅，恢复访问权限",
		"username", username,
		"time", time.Now().Format("2006-01-02 15:04:05"),
	)
}

// RecordSubscriptionAccessWithIP records subscription access and enables global access for all IPs
func (m *SilentModeManager) RecordSubscriptionAccessWithIP(username, ip string) {
	if username == "" {
		return
	}
	now := time.Now()
	m.lastActiveTime.Store(username, now)

	// 更新全局活跃时间，允许所有IP访问
	m.globalActiveMu.Lock()
	m.lastGlobalActiveTime = now
	m.globalActiveMu.Unlock()

	logger.Info("🔓 [SILENT_MODE] 用户获取订阅，恢复所有IP访问权限",
		"username", username,
		"ip", ip,
		"time", now.Format("2006-01-02 15:04:05"),
	)
}

func (m *SilentModeManager) isUserActive(username string, timeout int) bool {
	if username == "" {
		return false
	}

	val, ok := m.lastActiveTime.Load(username)
	if !ok {
		return false
	}

	lastActive := val.(time.Time)
	activeUntil := lastActive.Add(time.Duration(timeout) * time.Minute)
	return time.Now().Before(activeUntil)
}

// isGlobalActive checks if any user has fetched subscription recently (allows all IPs)
func (m *SilentModeManager) isGlobalActive(timeout int) bool {
	m.globalActiveMu.Lock()
	lastActive := m.lastGlobalActiveTime
	m.globalActiveMu.Unlock()

	if lastActive.IsZero() {
		return false
	}

	activeUntil := lastActive.Add(time.Duration(timeout) * time.Minute)
	return time.Now().Before(activeUntil)
}

// contoken获取用户名
func (m *SilentModeManager) extractUsername(r *http.Request) string {
	if m.tokens == nil {
		return ""
	}

	if token := strings.TrimSpace(r.Header.Get(auth.AuthHeader)); token != "" {
		if username, ok := m.tokens.Lookup(token); ok {
			return username
		}
	}

	if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		if username, ok := m.tokens.Lookup(token); ok {
			return username
		}
	}

	return ""
}

func (m *SilentModeManager) isAllowedPath(path string) bool {
	// 订阅相关接口始终可访问
	allowedPrefixes := []string{
		"/api/clash/subscribe",
		"/api/proxy-provider/",
		"/t/", // 临时订阅
	}

	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	// 短链接：精确匹配已知的短链接组合
	trimmedPath := strings.Trim(path, "/")
	if m.isKnownShortLink(trimmedPath) {
		return true
	}

	return false
}

func isAlphanumericPath(s string) bool {
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func (m *SilentModeManager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg, err := m.repo.GetSystemConfig(context.Background())
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}

		if !cfg.SilentMode {
			next.ServeHTTP(w, r)
			return
		}

		// 服务启动后的恢复期内，允许所有请求
		recoveryUntil := m.startTime.Add(time.Duration(cfg.SilentModeTimeout) * time.Minute)
		if time.Now().Before(recoveryUntil) {
			next.ServeHTTP(w, r)
			return
		}

		if m.isAllowedPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		username := m.extractUsername(r)
		clientIP := GetClientIP(r)

		// 检查用户是否在活跃期内（通过 token 识别）
		if username != "" && m.isUserActive(username, cfg.SilentModeTimeout) {
			next.ServeHTTP(w, r)
			return
		}

		// 检查是否有用户获取过订阅（允许所有IP访问）
		if m.isGlobalActive(cfg.SilentModeTimeout) {
			next.ServeHTTP(w, r)
			return
		}

		logger.Info("🔒 [SILENT_MODE] 请求被拦截",
			"path", r.URL.Path,
			"username", username,
			"client_ip", clientIP,
		)
		w.Header().Set("X-Silent-Mode", "true")
		http.NotFound(w, r)
	})
}
