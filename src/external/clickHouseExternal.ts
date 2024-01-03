import { PlywoodRequester } from "@topgames/plywood-base-api";
import * as toArray from "stream-to-array";
import { AttributeInfo, Attributes, PseudoDatum } from "../datatypes/index";
import { ClickHouseDialect } from "../dialect/clickHouseDialect";
import { PlyType } from "../types";
import { External, ExternalJS, ExternalValue } from "./baseExternal";
import { SQLExternal } from "./sqlExternal";

export interface ClickHouseDescribeRow {
  name: string;
  type: string;
}

export class ClickHouseExternal extends SQLExternal {
  static engine = "clickhouse";
  static type = "DATASET";

  static fromJS(
    parameters: ExternalJS,
    requester: PlywoodRequester<any>
  ): ClickHouseExternal {
    let value: ExternalValue = External.jsToValue(parameters, requester);
    return new ClickHouseExternal(value);
  }

  static postProcessIntrospect(columns: ClickHouseDescribeRow[]): Attributes {
    return columns
      .map((column: ClickHouseDescribeRow) => {
        let name = column.name;
        let type: PlyType;
        let nativeType = column.type.toLowerCase();
        if (
          nativeType.indexOf("datetime") === 0 ||
          nativeType.indexOf("date") === 0
        ) {
          type = "TIME";
        } else if (
          nativeType.indexOf("string") === 0 ||
          nativeType.indexOf("fixedstring") === 0 ||
          nativeType.indexOf("enum") === 0 ||
          nativeType.indexOf("uuid") === 0
        ) {
          type = "STRING";
        } else if (nativeType.indexOf("bool") === 0) {
          type = "BOOLEAN";
        } else if (
          nativeType.indexOf("int") === 0 ||
          nativeType.indexOf("uint") === 0 ||
          nativeType.indexOf("decimal") === 0 ||
          nativeType.indexOf("float") === 0
        ) {
          type = "NUMBER";
        } else {
          return null;
        }
        return new AttributeInfo({
          name,
          type,
          nativeType,
        });
      })
      .filter(Boolean);
  }

  static getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    return toArray(requester({ query: "SHOW TABLES" })).then((sources) => {
      if (!Array.isArray(sources)) throw new Error("invalid sources response");
      if (!sources.length) return sources;
      let key = Object.keys(sources[0])[0];
      if (!key) throw new Error("invalid sources response (no key)");
      return sources.map((s: PseudoDatum) => s[key]).sort();
    });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(requester({ query: "SELECT version()" })).then((res) => {
      if (!Array.isArray(res) || res.length !== 1)
        throw new Error("invalid version response");
      let key = Object.keys(res[0])[0];
      if (!key) throw new Error("invalid version response (no key)");
      return res[0][key];
    });
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new ClickHouseDialect());
    this._ensureEngine("clickhouse");
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    return toArray(
      this.requester({
        query: `DESCRIBE ${this.dialect.escapeName(this.source as string)}`,
      })
    ).then(ClickHouseExternal.postProcessIntrospect);
  }

  protected capability(cap: string): boolean {
    if (cap === "filter-on-attribute" || cap === "shortcut-group-by")
      return false;
    if (cap === "string-group-by") return true;
    return super.capability(cap);
  }
}

External.register(ClickHouseExternal);
