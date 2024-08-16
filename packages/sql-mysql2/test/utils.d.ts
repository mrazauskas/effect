import { MysqlClient } from "@effect/sql-mysql2";
import type { StartedMySqlContainer } from "@testcontainers/mysql";
import { Context, Layer } from "effect";
declare const ContainerError_base: new <A extends Record<string, any> = {}>(args: import("effect/Types").Equals<A, {}> extends true ? void : { readonly [P in keyof A as P extends "_tag" ? never : P]: A[P]; }) => import("effect/Cause").YieldableError & {
    readonly _tag: "ContainerError";
} & Readonly<A>;
export declare class ContainerError extends ContainerError_base<{
    cause: unknown;
}> {
}
declare const MysqlContainer_base: Context.TagClass<MysqlContainer, "test/MysqlContainer", StartedMySqlContainer>;
export declare class MysqlContainer extends MysqlContainer_base {
    static Live: Layer.Layer<MysqlContainer, ContainerError, never>;
    static ClientLive: Layer.Layer<MysqlClient.MysqlClient | import("@effect/sql/SqlClient").SqlClient, ContainerError | import("effect/ConfigError").ConfigError | import("@effect/sql/SqlError").SqlError, never>;
}
export {};
//# sourceMappingURL=utils.d.ts.map