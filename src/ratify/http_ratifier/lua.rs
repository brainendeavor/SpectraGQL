use crate::payload::RequestInfo;
use crate::ratify::RequestRatification;
use anyhow::{Result, anyhow};
use mlua::prelude::*;

#[derive(Clone)]
pub struct HttpRequestRatifierLua {}

impl HttpRequestRatifierLua {
    pub fn new() -> Self {
        HttpRequestRatifierLua {}
    }

    pub fn invoke_lua(&self, request_info: &RequestInfo) -> LuaResult<bool> {
        let lua = Lua::new();

        let lua_request_info = request_info.clone().into_lua(&lua)?;
        lua.globals().set("request_info", lua_request_info)?;

        let ratification_table = lua.create_table()?;
        ratification_table.set("request_ok", true)?;
        lua.globals().set("ratify", ratification_table)?;

        lua.load(
            "
            print(\"request_info: \", request_info.request_id)
            print(\"request_info.http: \", request_info.http.method, request_info.http.uri, request_info.http.version)

            for k,v in pairs(request_info.http.headers) do
                print(k,v)
            end

            -- ratify.request_ok = false
            ratify.request_ok = true
            ",
        )
        .exec()?;
        let ratify_table: LuaTable = lua.globals().get("ratify")?;
        let request_ok: bool = ratify_table.get("request_ok")?;
        Ok(request_ok)
    }
}

impl RequestRatification for HttpRequestRatifierLua {
    fn ratify_request(
        &self,
        request_id: uuid::Uuid,
        hlc: crate::clock::HlcTimestamp,
        http_request_parts: http::request::Parts,
        _http_request_body: &str,
    ) -> Result<RequestInfo> {
        let request_info = RequestInfo::new(request_id, hlc, http_request_parts);
        log::info!("enum_dispatch RequestRatification -> HttpRequestRatifierLua!");
        match self.invoke_lua(&request_info) {
            Ok(ratified) => {
                if ratified {
                    return Ok(request_info);
                } else {
                    return Err(anyhow!("Failed to ratify"));
                }
            }
            Err(e) => {
                return Err(anyhow!("Failed to ratify: {}", e));
            }
        }
    }
}
