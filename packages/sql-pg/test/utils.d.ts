import { PgClient } from "@effect/sql-pg";
import { Effect, Layer } from "effect";
declare const ContainerError_base: new <A extends Record<string, any> = {}>(args: import("effect/Types").Equals<A, {}> extends true ? void : { readonly [P in keyof A as P extends "_tag" ? never : P]: A[P]; }) => import("effect/Cause").YieldableError & {
    readonly _tag: "ContainerError";
} & Readonly<A>;
export declare class ContainerError extends ContainerError_base<{
    cause: unknown;
}> {
}
declare const PgContainer_base: Effect.Service.Class<PgContainer, "test/PgContainer", {
    readonly scoped: Effect.Effect<import("@testcontainers/postgresql").StartedPostgreSqlContainer, ContainerError, import("effect/Scope").Scope>;
}>;
export declare class PgContainer extends PgContainer_base {
    static ClientLive: Layer.Layer<PgClient.PgClient | import("@effect/sql/SqlClient").SqlClient, ContainerError | import("effect/ConfigError").ConfigError | import("@effect/sql/SqlError").SqlError, never>;
    static ClientTransformLive: Layer.Layer<PgClient.PgClient | import("@effect/sql/SqlClient").SqlClient, ContainerError | import("effect/ConfigError").ConfigError | import("@effect/sql/SqlError").SqlError, never>;
}
export {};
//# sourceMappingURL=utils.d.ts.map