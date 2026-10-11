import { describe, expect, test } from "bun:test"
import { attachmentPickerErrorToast } from "./model"

describe("attachment picker errors", () => {
  test("uses the plain IPC failure message in the error toast", () => {
    expect(
      attachmentPickerErrorToast(
        { t: () => "Request failed" },
        { message: "Selected attachments exceed the 20 MB limit" },
      ),
    ).toEqual({
      variant: "error",
      title: "Request failed",
      description: "Selected attachments exceed the 20 MB limit",
    })
  })
})
