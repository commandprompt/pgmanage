// Define all global stubs here
import { vi } from "vitest";

vi.stubGlobal("app_base_path", "test_folder");
vi.stubGlobal("v_csrf_cookie_name", "csrf_token_stub");

vi.mock("axios");
