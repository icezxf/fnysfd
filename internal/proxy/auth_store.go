// LoginViaEmby 通过 Emby 兼容的认证端点主动登录，获取 AccessToken
// 这是"路线二"的核心：利用飞牛影视的 Emby 兼容 API 主动认证
func (a *AuthStore) LoginViaEmby(username, password, serverURL string) error {
    if username == "" || password == "" {
        return fmt.Errorf("用户名或密码为空")
    }

    // 构造登录请求体（Emby 标准格式）
    loginData := map[string]string{
        "Username": username,
        "Pw":       password,
    }
    jsonData, err := json.Marshal(loginData)
    if err != nil {
        return fmt.Errorf("构造请求体失败: %w", err)
    }

    // 构造 Emby 认证头
    embyAuth := `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0"`

    // 尝试多个可能的端点（不同版本的飞牛可能路径不同）
    endpoints := []string{
        "/emby/Users/AuthenticateByName",
        "/Users/AuthenticateByName",
        "/emby/Users/authenticatebyname",
    }

    var lastErr error
    for _, endpoint := range endpoints {
        fullURL := serverURL + endpoint
        req, err := http.NewRequest("POST", fullURL, bytes.NewBuffer(jsonData))
        if err != nil {
            lastErr = err
            continue
        }
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("X-Emby-Authorization", embyAuth)
        req.Header.Set("Accept", "application/json")

        client := &http.Client{Timeout: 15 * time.Second}
        resp, err := client.Do(req)
        if err != nil {
            lastErr = fmt.Errorf("%s 请求失败: %w", endpoint, err)
            continue
        }

        body, _ := io.ReadAll(resp.Body)
        resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            lastErr = fmt.Errorf("%s 返回 %d: %s", endpoint, resp.StatusCode, string(body))
            continue
        }

        // 解析响应
        var loginResp struct {
            User struct {
                Id   string `json:"Id"`
                Name string `json:"Name"`
            } `json:"User"`
            AccessToken string `json:"AccessToken"`
        }
        if err := json.Unmarshal(body, &loginResp); err != nil {
            lastErr = fmt.Errorf("%s 解析失败: %w, body=%s", endpoint, err, string(body))
            continue
        }

        if loginResp.AccessToken == "" || loginResp.User.Id == "" {
            lastErr = fmt.Errorf("%s 响应缺少 AccessToken 或 User.Id", endpoint)
            continue
        }

        // 成功！存入缓存
        a.mu.Lock()
        defer a.mu.Unlock()
        a.headers = make(http.Header)
        a.headers.Set("X-Emby-Token", loginResp.AccessToken)
        a.headers.Set("X-Emby-Authorization", `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0", Token="`+loginResp.AccessToken+`"`)
        a.userID = loginResp.User.Id
        a.updatedAt = time.Now()

        return nil
    }

    return fmt.Errorf("所有端点均失败: %w", lastErr)
}
