import os
import json
import csv
import io
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pyperclip

DEFAULT_BATCH_SIZE = 10

def _split_single_text_cell(s):
    reader = csv.reader(io.StringIO(s), delimiter=",", skipinitialspace=True)
    for row in reader:
        return [c.strip() for c in row]
    return [s.strip()]

def chunk_rows(rows, batch_size):
    return [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

class ExcelFreeJsonTool(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("数据治理平台二维数组JSON生成工具")
        self.geometry("960x720")
        self.resizable(False, False)

        self.batch_size_var = tk.IntVar(value=DEFAULT_BATCH_SIZE)
        self.out_dir_var = tk.StringVar(value="JSON输出文件夹")
        self.file_list = []
        self.all_json_data = {}

        main = ttk.Frame(self, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        # 第一行：分组数量 + 输出目录
        line1 = ttk.Frame(main)
        line1.pack(fill="x", pady=(0,8))
        ttk.Label(line1, text="单文件最大行数：").pack(side="left")
        ttk.Spinbox(line1, textvariable=self.batch_size_var, from_=1, to=200, width=10).pack(side="left", padx=6)
        ttk.Label(line1, text="JSON保存目录：").pack(side="left", padx=20)
        ttk.Entry(line1, textvariable=self.out_dir_var, width=42).pack(side="left", padx=6)
        ttk.Button(line1, text="选择目录", command=self.select_out_dir).pack(side="left")

        # 第二块：参数输入区域
        ttk.Label(main, text="粘贴逗号分隔参数（一行一条完整参数）：").pack(anchor="w", pady=(6,2))
        self.input_text = tk.Text(main, width=116, height=10)
        self.input_text.pack()

        # 操作按钮行1
        btn_frame1 = ttk.Frame(main)
        btn_frame1.pack(pady=8)
        ttk.Button(btn_frame1, text="清空输入", command=self.clear_input).pack(side="left", padx=5)
        ttk.Button(btn_frame1, text="一键批量生成全部JSON", command=self.run_convert).pack(side="left", padx=5)
        ttk.Button(btn_frame1, text="打开输出文件夹", command=self.open_folder).pack(side="left", padx=5)

        # 第三块：分组切换 + JSON预览区（新增核心面板）
        preview_frame = ttk.LabelFrame(main, text="分组JSON预览（生成完成后切换查看、复制）")
        preview_frame.pack(fill="x", pady=(10,5))

        switch_line = ttk.Frame(preview_frame)
        switch_line.pack(fill="x", pady=4)
        ttk.Label(switch_line, text="选择分组文件：").pack(side="left")
        self.file_combo = ttk.Combobox(switch_line, values=[], state="readonly", width=22)
        self.file_combo.pack(side="left", padx=6)
        self.file_combo.bind("<<ComboboxSelected>>", self.on_switch_file)
        ttk.Button(switch_line, text="复制当前分组JSON", command=self.copy_current_json).pack(side="left", padx=20)

        self.preview_text = tk.Text(preview_frame, width=116, height=12)
        self.preview_text.pack(pady=4)

        # 日志区域
        ttk.Label(main, text="运行日志：").pack(anchor="w", pady=(6,2))
        self.log_text = tk.Text(main, width=116, height=6)
        self.log_text.pack()

    def log(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)
        self.update()

    def select_out_dir(self):
        d = filedialog.askdirectory()
        if d:
            self.out_dir_var.set(d)
            self.log(f"已设置输出目录：{d}")

    def open_folder(self):
        d = self.out_dir_var.get()
        if not os.path.exists(d):
            os.makedirs(d)
            self.log(f"自动创建目录：{d}")
        os.startfile(d)

    def clear_input(self):
        self.input_text.delete("1.0", tk.END)
        self.log("已清空参数输入框")

    def on_switch_file(self, event):
        """切换下拉分组，加载对应JSON到预览框"""
        selected_name = self.file_combo.get()
        if not selected_name:
            return
        json_str = self.all_json_data[selected_name]
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert("1.0", json_str)

    def copy_current_json(self):
        """复制预览框内当前分组JSON到剪贴板"""
        content = self.preview_text.get("1.0", tk.END).strip()
        if not content:
            messagebox.showwarning("提示", "暂无可复制的JSON内容，请先生成分组")
            return
        pyperclip.copy(content)
        self.log("✅ 当前分组二维数组JSON已复制到剪贴板，直接粘贴华为云批作业！")
        messagebox.showinfo("复制成功", "当前分组JSON已复制剪贴板")

    def run_convert(self):
        try:
            raw = self.input_text.get("1.0", tk.END)
            raw = raw.replace("\r\n", "\n").replace("\r", "\n")
            raw_lines = raw.splitlines()
            temp_lines = []
            current_full_line = ""

            for line in raw_lines:
                line_strip = line.strip()
                if not line_strip:
                    continue
                if current_full_line != "":
                    current_full_line += line_strip
                    if current_full_line.count(",") >= 9:
                        temp_lines.append(current_full_line)
                        current_full_line = ""
                else:
                    current_full_line = line_strip
                    if current_full_line.count(",") >= 9:
                        temp_lines.append(current_full_line)
                        current_full_line = ""
            if current_full_line.strip():
                temp_lines.append(current_full_line)

            clean_param_lines = temp_lines
            if len(clean_param_lines) == 0:
                messagebox.showwarning("提示", "未识别到完整参数！复制内容存在换行断裂")
                self.log("❌ 无有效完整参数，请重新粘贴")
                return
            self.log(f"✅ 清洗完成，共识别 {len(clean_param_lines)} 条完整参数")

            batch_size = self.batch_size_var.get()
            if batch_size <= 0:
                messagebox.showerror("错误", "每组行数必须大于0")
                return

            origin_rows = []
            for idx, param_str in enumerate(clean_param_lines, 1):
                row_arr = _split_single_text_cell(param_str)
                origin_rows.append(row_arr)
                self.log(f"第{idx}条，字段总数：{len(row_arr)}")

            max_col = max(len(r) for r in origin_rows)
            norm_rows = []
            for r in origin_rows:
                fill = [""] * (max_col - len(r))
                norm_rows.append(r + fill)
            self.log(f"✅ 统一所有行列数为 {max_col}")

            batches = chunk_rows(norm_rows, batch_size)
            total_batch = len(batches)
            self.log(f"按每组{batch_size}条拆分，共生成{total_batch}个分组文件")

            out_dir = self.out_dir_var.get()
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            self.file_list.clear()
            self.all_json_data.clear()

            for idx, batch_data in enumerate(batches, start=1):
                json_str = json.dumps(batch_data, ensure_ascii=False, separators=(",", ":"))
                fname = f"结果_{idx:03d}.json"
                full_path = os.path.join(out_dir, fname)
                with open(full_path, "w", encoding="utf-8") as f:
                    f.write(json_str)
                self.log(f"✅ 生成文件：{fname}，内含{len(batch_data)}条数据")
                self.file_list.append(fname)
                self.all_json_data[fname] = json_str

            # 刷新下拉选择框，默认加载第一组
            self.file_combo["values"] = self.file_list
            if self.file_list:
                self.file_combo.current(0)
                self.on_switch_file(None)

            self.log("\n🎉 全部处理完成！上方下拉框可切换任意分组预览、一键复制")
            messagebox.showinfo("执行完成", f"总参数：{len(origin_rows)}条\n生成分组：{total_batch}个\n可在工具内切换预览并复制任意分组JSON")
        except Exception as e:
            err = f"程序运行异常：{str(e)}"
            self.log(err)
            messagebox.showerror("失败", err)

if __name__ == "__main__":
    app = ExcelFreeJsonTool()
    app.mainloop()