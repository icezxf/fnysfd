package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// 硬编码开关（先不做配置，测试通过后再提取到 config）
// ============================================================

// ✅ 先硬编码为 true，测试用。成功后改成 config.GetEnableDoubanRating()
var doubanEnabled = true

const (
	doubanDefaultApiKey      = "0ab215a8b1977939201640fa14c66bab"
	doubanTVApiHost          = "https://douban-idatabase.kfstorm.com"
	doubanDBPath             = "./data/douban_cache.json"
	doubanMinRequestInterval = 2 * time.Second
	doubanLogPrefix          = "🎬 [豆瓣评分]"
)

// ============================================================
// API 响应结构
// ============================================================

type doubanMovieApiResponse struct {
	Rating struct {
		Average string `json:"average"`
	} `json:"rating"`
	Title string `json:"title"`
	Msg   string `json:"msg"`
	Code  int    `json:"code"`
}

type doubanSubjectApiResponse struct {
	Rating struct {
		Average float64 `json:"average"`
	} `json:"rating"`
	Title string `json:"title"`
	Msg   string `json:"msg"`
	Code  int    `json:"code"`
}

type doubanTvApiItem struct {
	DoubanID string  `json:"douban_id"`
	Rating   float64 `json:"rating"`
}

type doubanSuggestItem struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Type  string `json:"type"`
	Year  string `json:"year"`
}

// ============================================================
// 缓存结构
// ============================================================

type doubanCacheEntry struct {
	Rating    float64   `json:"rating"`
	Title     string    `json:"title,omitempty"`
	FetchedAt time.Time `json:"fetchedAt"`
}

type doubanDBFile struct {
	Version   int                          `json:"version"`
	UpdatedAt time.Time                    `json:"updatedAt"`
	Entries   map[string]*doubanCacheEntry `json:"entries"`
}

// ============================================================
// DoubanProvider
// ============================================================

// DoubanProvider 豆瓣评分提供器
//
// 职责：
//   1. FetchMovieOrSeries / FetchSeason：抓取评分（供 LibraryScanner 调用）
//   2. GetRating / GetSeasonRating：查询评分（供反代注入调用）
//   3. InjectInto：把评分注入到 item map
//   4. 磁盘持久化（data/douban_cache.json）
type DoubanProvider struct {
	server     *Server
	httpClient *http.Client
	apiKey     string

	// 极简内存索引：key → rating
	indexMu sync.RWMutex
	index   map[string]float32

	// 磁盘数据
	diskMu      sync.RWMutex
	diskEntries map[string]*doubanCacheEntry
	dirty       atomic.Bool

	// 写盘串行锁
	flushMu sync.Mutex

	// 豆瓣请求全局限速
	rateMu  sync.Mutex
	lastReq time.Time

	// 统计
	statHits    atomic.Int64
	statMisses  atomic.Int64
	statFetched atomic.Int64
	statFailed  atomic.Int64

	// 生命周期
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewDoubanProvider 创建豆瓣评分提供器
func NewDoubanProvider(s *Server) *DoubanProvider {
	apiKey := os.Getenv("DOUBAN_API_KEY")
	if apiKey == "" {
		apiKey = doubanDefaultApiKey
	}
	dp := &DoubanProvider{
		server:      s,
		httpClient:  &http.Client{Timeout: 15 * time.Second},
		apiKey:      apiKey,
		index:       make(map[string]float32),
		diskEntries: make(map[string]*doubanCacheEntry),
		stopCh:      make(chan struct{}),
	}
	// 加载磁盘缓存
	if err := dp.loadDB(); err != nil {
		s.logger.Warn("%s 加载缓存失败: %v", doubanLogPrefix, err)
	} else {
		dp.indexMu.RLock()
		n := len(dp.index)
		dp.indexMu.RUnlock()
		s.logger.Info("%s 缓存已加载: %d 条", doubanLogPrefix, n)
	}
	// 启动定期写盘
	dp.wg.Add(1)
	go func() {
		defer dp.wg.Done()
		dp.flushLoop()
	}()
	s.logger.Info("%s 组件就绪（抓取由增量扫描驱动）", doubanLogPrefix)
	return dp
}

// Stop 停止
func (dp *DoubanProvider) Stop() {
	dp.stopOnce.Do(func() { close(dp.stopCh) })
	dp.wg.Wait()
	if dp.dirty.Load() {
		_ = dp.saveDB()
	}
}

// ============================================================
// 抓取接口（供 LibraryScanner 调用）
// ============================================================

// FetchMovieOrSeries 抓取电影/剧集评分（异步，调用方应 go 调用）
//
// 优先用 IMDb ID 查（官方 API / 第三方 API），
// 无 IMDb 或查询失败时回退到标题搜索。
func (dp *DoubanProvider) FetchMovieOrSeries(ctx context.Context, imdbID, name string, year int, itemType string) {
	if !doubanEnabled {
		return
	}
	if imdbID == "" && name == "" {
		return
	}
	if dp.hasCache(imdbID, name, year) {
		return
	}

	var rating float64
	var err error

	// 1. 优先 IMDb
	if imdbID != "" {
		if itemType == "Series" {
			rating, err = dp.getTVRatingByImdb(ctx, imdbID)
		} else {
			rating, err = dp.getMovieRatingByImdb(ctx, imdbID)
		}
		if err == nil && rating > 0 {
			dp.storeCache(imdbID, name, year, rating)
			dp.statFetched.Add(1)
			dp.server.logger.Info("%s ✅ %s (IMDb=%s) → %.1f", doubanLogPrefix, name, imdbID, rating)
			return
		}
	}

	// 2. 标题搜
	if name != "" {
		rating, err = dp.searchByTitle(ctx, name, year, itemType)
		if err == nil && rating > 0 {
			dp.storeCache(imdbID, name, year, rating)
			dp.statFetched.Add(1)
			dp.server.logger.Info("%s ✅ %s (标题搜) → %.1f", doubanLogPrefix, name, rating)
			return
		}
	}

	dp.statFailed.Add(1)
	dp.server.logger.Debug("%s ❌ %s 未匹配", doubanLogPrefix, name)
}

// FetchSeason 抓取季评分（供 LibraryScanner 调用）
func (dp *DoubanProvider) FetchSeason(ctx context.Context, seriesName string, seasonNumber int) {
	if !doubanEnabled {
		return
	}
	if seriesName == "" || seasonNumber <= 0 {
		return
	}

	key := fmt.Sprintf("season:%s|%d", seriesName, seasonNumber)
	if dp.hasCacheKey(key) {
		return
	}

	rating, err := dp.searchSeasonRating(ctx, seriesName, seasonNumber)
	if err != nil || rating <= 0 {
		dp.statFailed.Add(1)
		dp.server.logger.Debug("%s ❌ 季 %s 第%d季 未匹配", doubanLogPrefix, seriesName, seasonNumber)
		return
	}

	dp.storeCacheByKey(key, seriesName, rating)
	dp.statFetched.Add(1)
	dp.server.logger.Info("%s ✅ %s 第%d季 → %.1f", doubanLogPrefix, seriesName, seasonNumber, rating)
}

// ============================================================
// 查询接口（供反代注入调用）
// ============================================================

func (dp *DoubanProvider) GetRating(imdbID, name string, year int) (float64, bool) {
	dp.indexMu.RLock()
	defer dp.indexMu.RUnlock()

	if imdbID != "" {
		if r, ok := dp.index["imdb:"+imdbID]; ok {
			dp.statHits.Add(1)
			return float64(r), true
		}
	}
	if name != "" {
		key := fmt.Sprintf("title:%s|%d", cleanTitle(name), year)
		if r, ok := dp.index[key]; ok {
			dp.statHits.Add(1)
			return float64(r), true
		}
	}
	dp.statMisses.Add(1)
	return 0, false
}

func (dp *DoubanProvider) GetSeasonRating(seriesName string, seasonNumber int) (float64, bool) {
	dp.indexMu.RLock()
	defer dp.indexMu.RUnlock()

	key := fmt.Sprintf("season:%s|%d", seriesName, seasonNumber)
	if r, ok := dp.index[key]; ok {
		dp.statHits.Add(1)
		return float64(r), true
	}
	dp.statMisses.Add(1)
	return 0, false
}

// ============================================================
// 反代注入
// ============================================================

// InjectInto 把豆瓣评分注入到 item map（供 server.go 的 handleResponse 调用）
//
// 策略：
//   - Movie / Series：覆盖 CommunityRating
//   - Season：尝试覆盖 CommunityRating（客户端可能不认）+ Overview 前缀兜底
//   - 其他：跳过
func (dp *DoubanProvider) InjectInto(item map[string]interface{}) {
	if !doubanEnabled {
		return
	}

	itemType, _ := item["Type"].(string)
	switch itemType {
	case "Movie", "Series":
		dp.injectMovieOrSeries(item)
	case "Season":
		dp.injectSeason(item)
	}
}

func (dp *DoubanProvider) injectMovieOrSeries(item map[string]interface{}) {
	name, _ := item["Name"].(string)
	if name == "" {
		return
	}

	year := 0
	if y, ok := item["ProductionYear"].(float64); ok {
		year = int(y)
	}

	imdbID := ""
	if pids, ok := item["ProviderIds"].(map[string]interface{}); ok {
		if v, ok := pids["Imdb"].(string); ok {
			imdbID = v
		}
	}

	rating, ok := dp.GetRating(imdbID, name, year)
	if !ok || rating <= 0 {
		return
	}

	item["CommunityRating"] = rating
	dp.server.logger.Debug("%s 注入 %s: CommunityRating=%.1f", doubanLogPrefix, name, rating)
}

func (dp *DoubanProvider) injectSeason(item map[string]interface{}) {
	seriesName, _ := item["SeriesName"].(string)
	if seriesName == "" {
		return
	}

	indexNumber := 0
	if n, ok := item["IndexNumber"].(float64); ok {
		indexNumber = int(n)
	}
	if indexNumber <= 0 {
		return
	}

	rating, ok := dp.GetSeasonRating(seriesName, indexNumber)
	if !ok || rating <= 0 {
		return
	}

	// 尝试覆盖 CommunityRating（部分客户端可能不认）
	item["CommunityRating"] = rating

	// Overview 前缀兜底
	orig, _ := item["Overview"].(string)
	prefix := fmt.Sprintf("【豆瓣 ⭐%.1f】\n\n", rating)
	if !strings.HasPrefix(orig, "【豆瓣") {
		item["Overview"] = prefix + orig
	}

	dp.server.logger.Debug("%s 注入季 %s 第%d季: %.1f", doubanLogPrefix, seriesName, indexNumber, rating)
}

// ============================================================
// 豆瓣 API
// ============================================================

func (dp *DoubanProvider) globalRateLimit(ctx context.Context) {
	dp.rateMu.Lock()
	now := time.Now()
	var wait time.Duration
	if dp.lastReq.After(now) {
		wait = dp.lastReq.Sub(now)
		dp.lastReq = dp.lastReq.Add(doubanMinRequestInterval)
	} else {
		wait = 0
		dp.lastReq = now.Add(doubanMinRequestInterval)
	}
	dp.rateMu.Unlock()

	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		case <-dp.stopCh:
		}
	}
}

func (dp *DoubanProvider) getMovieRatingByImdb(ctx context.Context, imdbID string) (float64, error) {
	dp.globalRateLimit(ctx)

	apiURL := fmt.Sprintf("https://api.douban.com/v2/movie/imdb/%s", imdbID)
	formData := url.Values{}
	formData.Set("apikey", dp.apiKey)

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

	resp, err := dp.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status=%d", resp.StatusCode)
	}

	var result doubanMovieApiResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	if result.Code != 0 {
		return 0, fmt.Errorf("api code=%d msg=%s", result.Code, result.Msg)
	}

	ratingStr := strings.TrimSpace(result.Rating.Average)
	if ratingStr == "" {
		return 0, fmt.Errorf("empty rating")
	}
	return strconv.ParseFloat(ratingStr, 64)
}

func (dp *DoubanProvider) getTVRatingByImdb(ctx context.Context, imdbID string) (float64, error) {
	dp.globalRateLimit(ctx)

	apiURL := fmt.Sprintf("%s/api/item?imdb_id=%s", doubanTVApiHost, imdbID)
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

	resp, err := dp.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status=%d", resp.StatusCode)
	}

	var results []doubanTvApiItem
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return 0, err
	}
	if len(results) == 0 || results[0].Rating <= 0 {
		return 0, fmt.Errorf("empty")
	}
	return results[0].Rating, nil
}

func (dp *DoubanProvider) searchByTitle(ctx context.Context, name string, year int, itemType string) (float64, error) {
	dp.globalRateLimit(ctx)

	searchURL := "https://movie.douban.com/j/subject_suggest?q=" + url.QueryEscape(cleanTitle(name))
	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
	req.Header.Set("Referer", "https://movie.douban.com/")

	resp, err := dp.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status=%d", resp.StatusCode)
	}

	var results []doubanSuggestItem
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return 0, err
	}
	if len(results) == 0 {
		return 0, fmt.Errorf("no results")
	}

	wantType := "movie"
	if itemType == "Series" {
		wantType = "tv"
	}

	var matchedID string
	for _, r := range results {
		if r.Type != wantType {
			continue
		}
		rYear, _ := strconv.Atoi(r.Year)
		if year > 0 && rYear > 0 && absInt(year-rYear) > 1 {
			continue
		}
		matchedID = r.ID
		break
	}
	if matchedID == "" {
		return 0, fmt.Errorf("no match")
	}

	return dp.fetchRatingByDoubanID(ctx, matchedID)
}

func (dp *DoubanProvider) searchSeasonRating(ctx context.Context, seriesName string, seasonNumber int) (float64, error) {
	dp.globalRateLimit(ctx)

	seasonStr := seasonNumberToChinese(seasonNumber)
	keyword := seriesName + " " + seasonStr

	searchURL := "https://movie.douban.com/j/subject_suggest?q=" + url.QueryEscape(keyword)
	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
	req.Header.Set("Referer", "https://movie.douban.com/")

	resp, err := dp.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status=%d", resp.StatusCode)
	}

	var results []doubanSuggestItem
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return 0, err
	}

	normalizedSeries := normalizeTitle(seriesName)
	for _, r := range results {
		if r.Type != "tv" {
			continue
		}
		if !strings.Contains(r.Title, seasonStr) {
			continue
		}
		if !strings.Contains(normalizeTitle(r.Title), normalizedSeries) {
			continue
		}
		return dp.fetchRatingByDoubanID(ctx, r.ID)
	}
	return 0, fmt.Errorf("no season match")
}

func (dp *DoubanProvider) fetchRatingByDoubanID(ctx context.Context, doubanID string) (float64, error) {
	dp.globalRateLimit(ctx)

	apiURL := fmt.Sprintf("https://api.douban.com/v2/movie/subject/%s", doubanID)
	formData := url.Values{}
	formData.Set("apikey", dp.apiKey)

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := dp.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status=%d", resp.StatusCode)
	}

	var result doubanSubjectApiResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	if result.Rating.Average <= 0 {
		return 0, fmt.Errorf("no rating")
	}
	return result.Rating.Average, nil
}

// ============================================================
// 缓存管理
// ============================================================

func (dp *DoubanProvider) hasCache(imdbID, name string, year int) bool {
	dp.indexMu.RLock()
	defer dp.indexMu.RUnlock()

	if imdbID != "" {
		if _, ok := dp.index["imdb:"+imdbID]; ok {
			return true
		}
	}
	if name != "" {
		key := fmt.Sprintf("title:%s|%d", cleanTitle(name), year)
		if _, ok := dp.index[key]; ok {
			return true
		}
	}
	return false
}

func (dp *DoubanProvider) hasCacheKey(key string) bool {
	dp.indexMu.RLock()
	defer dp.indexMu.RUnlock()
	_, ok := dp.index[key]
	return ok
}

func (dp *DoubanProvider) storeCache(imdbID, name string, year int, rating float64) {
	entry := &doubanCacheEntry{
		Rating:    rating,
		Title:     name,
		FetchedAt: time.Now(),
	}

	keys := make([]string, 0, 2)
	if imdbID != "" {
		keys = append(keys, "imdb:"+imdbID)
	}
	if name != "" {
		keys = append(keys, fmt.Sprintf("title:%s|%d", cleanTitle(name), year))
	}

	dp.indexMu.Lock()
	for _, k := range keys {
		dp.index[k] = float32(rating)
	}
	dp.indexMu.Unlock()

	dp.diskMu.Lock()
	for _, k := range keys {
		dp.diskEntries[k] = entry
	}
	dp.diskMu.Unlock()

	dp.dirty.Store(true)
}

func (dp *DoubanProvider) storeCacheByKey(key, title string, rating float64) {
	entry := &doubanCacheEntry{
		Rating:    rating,
		Title:     title,
		FetchedAt: time.Now(),
	}

	dp.indexMu.Lock()
	dp.index[key] = float32(rating)
	dp.indexMu.Unlock()

	dp.diskMu.Lock()
	dp.diskEntries[key] = entry
	dp.diskMu.Unlock()

	dp.dirty.Store(true)
}

// ============================================================
// 磁盘持久化
// ============================================================

func (dp *DoubanProvider) loadDB() error {
	data, err := os.ReadFile(doubanDBPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var db doubanDBFile
	if err := json.Unmarshal(data, &db); err != nil {
		return err
	}

	dp.indexMu.Lock()
	for k, entry := range db.Entries {
		dp.index[k] = float32(entry.Rating)
	}
	dp.indexMu.Unlock()

	dp.diskMu.Lock()
	dp.diskEntries = db.Entries
	if dp.diskEntries == nil {
		dp.diskEntries = make(map[string]*doubanCacheEntry)
	}
	dp.diskMu.Unlock()

	return nil
}

func (dp *DoubanProvider) saveDB() error {
	if !dp.dirty.CompareAndSwap(true, false) {
		return nil
	}

	dp.flushMu.Lock()
	defer dp.flushMu.Unlock()

	dp.diskMu.RLock()
	snapshot := doubanDBFile{
		Version:   1,
		UpdatedAt: time.Now(),
		Entries:   make(map[string]*doubanCacheEntry, len(dp.diskEntries)),
	}
	for k, v := range dp.diskEntries {
		snapshot.Entries[k] = v
	}
	dp.diskMu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(doubanDBPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpPath := doubanDBPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, doubanDBPath)
}

func (dp *DoubanProvider) flushLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if dp.dirty.Load() {
				if err := dp.saveDB(); err != nil {
					dp.server.logger.Warn("%s 写盘失败: %v", doubanLogPrefix, err)
				}
			}
		case <-dp.stopCh:
			return
		}
	}
}

// ============================================================
// 统计 / 工具
// ============================================================

func (dp *DoubanProvider) GetStats() map[string]interface{} {
	dp.indexMu.RLock()
	entryCount := len(dp.index)
	dp.indexMu.RUnlock()

	dp.diskMu.RLock()
	updatedAt := time.Time{}
	for _, e := range dp.diskEntries {
		if e.FetchedAt.After(updatedAt) {
			updatedAt = e.FetchedAt
		}
	}
	dp.diskMu.RUnlock()

	var fileSize int64
	if info, err := os.Stat(doubanDBPath); err == nil {
		fileSize = info.Size()
	}

	updatedStr := ""
	if !updatedAt.IsZero() {
		updatedStr = updatedAt.Format("2006-01-02 15:04:05")
	}

	return map[string]interface{}{
		"enabled":      doubanEnabled,
		"entries":      entryCount,
		"file_size_kb": fileSize / 1024,
		"updated_at":   updatedStr,
		"stat_hits":    dp.statHits.Load(),
		"stat_misses":  dp.statMisses.Load(),
		"stat_fetched": dp.statFetched.Load(),
		"stat_failed":  dp.statFailed.Load(),
	}
}

// ============================================================
// 工具函数
// ============================================================

func cleanTitle(name string) string {
	re := regexp.MustCompile(`\s*[\(\（]\d{4}[\)\）]\s*$`)
	name = re.ReplaceAllString(name, "")
	re2 := regexp.MustCompile(`\s*[\[\【][^\]\】]*[\]\】]`)
	name = re2.ReplaceAllString(name, "")
	return strings.TrimSpace(name)
}

func normalizeTitle(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "　", "")
	s = strings.ReplaceAll(s, "：", "")
	s = strings.ReplaceAll(s, ":", "")
	return strings.ToLower(s)
}

func seasonNumberToChinese(n int) string {
	m := map[int]string{
		1: "第一季", 2: "第二季", 3: "第三季", 4: "第四季", 5: "第五季",
		6: "第六季", 7: "第七季", 8: "第八季", 9: "第九季", 10: "第十季",
	}
	if s, ok := m[n]; ok {
		return s
	}
	return fmt.Sprintf("第%d季", n)
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
